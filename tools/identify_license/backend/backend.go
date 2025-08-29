// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package backend contains the necessary functions to classify a license.
package backend

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/tq-systems/em-lib-licenseclassifier"
	"github.com/tq-systems/em-lib-licenseclassifier/commentparser"
	"github.com/tq-systems/em-lib-licenseclassifier/commentparser/language"
	"github.com/tq-systems/em-lib-licenseclassifier/tools/identify_license/results"
)

// ClassifierInterface is the interface each backend must implement.
type ClassifierInterface interface {
	Close()
	ClassifyLicenses(filenames []string, headers bool) []error
	ClassifyLicensesWithContext(ctx context.Context, filenames []string, headers bool) []error
	GetResults() results.LicenseTypes
}

// ClassifierBackend is an object that handles classifying a license.
type ClassifierBackend struct {
	results    results.LicenseTypes
	mu         sync.Mutex
	classifier *licenseclassifier.License
}

// New creates a new backend working on the local filesystem.
func New(threshold float64, forbiddenOnly bool) (*ClassifierBackend, error) {
	var lc *licenseclassifier.License
	var err error
	if forbiddenOnly {
		lc, err = licenseclassifier.NewWithForbiddenLicenses(threshold)
	} else {
		lc, err = licenseclassifier.New(threshold)
	}
	if err != nil {
		return nil, err
	}
	return &ClassifierBackend{classifier: lc}, nil
}

// Close does nothing here since there's nothing to close.
func (b *ClassifierBackend) Close() {
}

// ClassifyLicenses runs the license classifier over the given file.
func (b *ClassifierBackend) ClassifyLicenses(filenames []string, headers bool) (errors []error) {
	return b.ClassifyLicensesWithContext(context.Background(), filenames, headers)
}

// ClassifyLicensesWithContext runs the license classifier over the given file;
// ensure that it will respect the timeout and cancelation in the provided context.
func (b *ClassifierBackend) ClassifyLicensesWithContext(ctx context.Context, filenames []string, headers bool) (errors []error) {
	files := make(chan string, len(filenames))
	for _, f := range filenames {
		files <- f
	}
	close(files)
	errs := make(chan error, len(filenames))

	var wg sync.WaitGroup

	// Create a pool from which tasks can later be started. We use a pool because the OS limits
	// the number of files that can be open at any one time.
	const numTasks = 1000
	wg.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		go func() {
			// Ensure that however this function terminates, the wait group
			// is unblocked
			defer wg.Done()

			for {
				filename := <-files

				// no file? we're done
				if filename == "" {
					break
				}

				// If the context is done, record that the file was not
				// classified due to the context's termination.
				if err := ctx.Err(); err != nil {
					errs <- fmt.Errorf("file %s not classified due to context completion: %v", filename, err)
					continue
				}

				if err := b.classifyLicense(filename, headers); err != nil {
					errs <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		errors = append(errors, err)
	}
	return errors
}

// classifyLicense is called by a Go-function to perform the actual
// classification of a license.
func (b *ClassifierBackend) classifyLicense(filename string, headers bool) error {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("unable to read %q: %v", filename, err)
	}

	matchLoop := func(contents string) {
		log.Printf("classifyLicense: contents=%v, headers=%v", contents, headers)
		for _, m := range b.classifier.MultipleMatch(contents, headers) {
			b.mu.Lock()
			if m.Name == "EPL-2.0" && strings.Contains(contents, "Eclipse Distribution License v1.0") {
				b.results = append(b.results, &results.LicenseType{
					Filename:   filename,
					Name:       "EDL-1.0",
					Confidence: 1,
					Offset:     0,
					Extent:     0,
				})
				log.Printf("use EDL-1.0 license")
			} else {
				b.results = append(b.results, &results.LicenseType{
					Filename:   filename,
					Name:       m.Name,
					Confidence: m.Confidence,
					Offset:     m.Offset,
					Extent:     m.Extent,
				})
				log.Printf("classifyLicense:matchLoop: Filename=%v, Name=%v, Confidence=%v, Offset=%v, Extent=%v", filename, m.Name, m.Confidence, m.Offset, m.Extent)
			}
			b.mu.Unlock()
		}
	}

	log.Printf("Classifying license(s): %s", filename)
	start := time.Now()
	if lang := language.ClassifyLanguage(filename); lang == language.Unknown {
		matchLoop(string(contents))
	} else {
		log.Printf("detected language: %v", lang)
		comments := commentparser.Parse(contents, lang)
		for ch := range comments.ChunkIterator() {
			matchLoop(ch.String())
		}
	}
	log.Printf("Finished Classifying License %q: %v", filename, time.Since(start))
	return nil
}

// GetResults returns the results of the classifications.
func (b *ClassifierBackend) GetResults() results.LicenseTypes {
	return b.results
}
