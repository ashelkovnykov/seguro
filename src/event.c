//! @file events.c
//!
//! Generates or reads events from LMDB, and loads them into memory.

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "constants.h"
#include "event.h"


//==============================================================================
// Functions
//==============================================================================

void fragment_event(Event *event, FragmentedEvent *f_event) {

  uint16_t payload_length = get_payload_length(event->data_length);
  uint32_t num_fragments = (event->data_length / payload_length);
  uint32_t simple_fragments = num_fragments;
  uint16_t final_frag_length = (event->data_length % payload_length);
  uint16_t  header_length = (OPTIMAL_VALUE_SIZE - payload_length);

  if (final_frag_length) {
    final_frag_length += header_length;
    ++num_fragments;
  }

  uint8_t **fragments = (uint8_t **) malloc(sizeof(uint8_t *) * num_fragments);
  for (uint32_t i = 0; i < simple_fragments; ++i) {
    fragments[i] = (uint8_t *) malloc(sizeof(uint8_t) * OPTIMAL_VALUE_SIZE);
    memcpy((fragments[i] + header_length), (event->data + (i * payload_length)), payload_length);
    build_fragment_header(fragments[i], header_length, num_fragments, i);
  }

  if (simple_fragments != num_fragments) {
    fragments[simple_fragments] = (uint8_t *) malloc(sizeof(uint8_t) * final_frag_length);
    memcpy((fragments[simple_fragments] + header_length), (event->data + (simple_fragments * payload_length)), final_frag_length);
    build_fragment_header(fragments[simple_fragments], header_length, num_fragments, simple_fragments);
  }

  f_event->key = event->key;
  f_event->num_fragments = num_fragments;
  f_event->final_frag_length = final_frag_length;
  f_event->fragments = fragments;
}

void build_fragment_header(uint8_t *fragment,
                           uint16_t header_length,
                           uint32_t num_fragments,
                           uint32_t current_fragment) {

  uint8_t i = 0;
  uint8_t x = (uint8_t)((header_length - 2) / 2);

  if (header_length == 4) {
    fragment[i++] = (uint8_t)num_fragments;
    i = 1;
  } else {
    fragment[i++] = (EXTENDED_HEADER & x);
    for (int j = (x - 2); j >= 0; --j) {
      fragment[i++] = ((uint8_t *)(&current_fragment))[j];
    }
  }

  fragment[i++] = ':';

  for (int j = (x - 1); j >= 0; --j) {
    fragment[i++] = ((uint8_t *)(&num_fragments))[j];
  }

  fragment[i++] = ':';
}

uint32_t get_payload_length(uint64_t data_length) {
  return (data_length <= 1269492)   ? 9996 :
         (data_length <= 2548470)   ? 9994 :
         (data_length <= 654825720) ? 9992 :
         9990;
}

void free_event(Event *event) {
  free((void *) event->data);
}

void free_fragmented_event(FragmentedEvent *event) {
  for (uint32_t i = 0; i < event->num_fragments; ++i) {
    free((void *) (event->fragments)[i]);
  }

  free((void *) event->fragments);
}
