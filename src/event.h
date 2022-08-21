//! @file events.h
//!
//! Event struct definition and utilities.

#pragma once

#include <foundationdb/fdb_c.h>
#include <stdint.h>

#define EXTENDED_HEADER 0x80
#define MAX_HEADER_SIZE 4
#define MAX_NUM_FRAGMENTS 16777216


//==============================================================================
// Types
//==============================================================================

typedef struct event_t {
  uint64_t  key;
  uint64_t  data_length;
  uint8_t  *data;
} Event;

typedef struct fragmented_event_t {
  uint64_t   key;
  uint32_t   num_fragments;
  uint16_t   final_frag_length;
  uint8_t  **fragments;
} FragmentedEvent;

//==============================================================================
// Prototypes
//==============================================================================

void fragment_event(Event *event, FragmentedEvent *f_event);

void build_fragment_header(uint8_t *fragment,
                           uint16_t header_length,
                           uint32_t num_fragments,
                           uint32_t current_fragment);

uint32_t get_payload_length(uint64_t data_length);

void free_event(Event *event);

void free_fragmented_event(FragmentedEvent *event);
