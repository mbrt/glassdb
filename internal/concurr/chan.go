// Copyright 2023 The glassdb Authors
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

package concurr

// MakeChainInfCap simulates a channel with infinite capacity. This will never
// block the sender and grow an internal buffer if necessary.
//
// Close the input channel when finished, to free resources.
func MakeChanInfCap[T any](expectedCap int) (<-chan T, chan<- T) {
	in := make(chan T, expectedCap)
	out := make(chan T, expectedCap)
	var queue []T

	go func() {
	loop:
		for {
			// Nothing to output here. We need to wait for some input.
			v, ok := <-in
			if !ok {
				// We're closing. Just exit (the queue is empty).
				break loop
			}
			// Try to push the element directly to the out chan.
			// If it's full, push it to the queue instead.
			select {
			case out <- v:
				continue loop
			default:
				queue = append(queue, v)
			}

			// Here we have something in the queue.
			for len(queue) > 0 {
				select {
				case v, ok := <-in:
					if !ok {
						// We're closing. Consume the whole queue and exit.
						for _, e := range queue {
							out <- e
						}
						break loop
					}
					queue = append(queue, v)
				case out <- queue[0]:
					var empty T
					queue[0] = empty // Avoid possible leaks.
					queue = queue[1:]
				}
			}
		}

		// Signal the end of things.
		close(out)
	}()

	return out, in
}
