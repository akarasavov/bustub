//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <iostream>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  this->access_states.resize(num_pages);
  this->pins_states.resize(num_pages);
  for (size_t i = 0; i < num_pages; i++) {
    pins_states[i] = true;
    access_states[i] = false;
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  lock.lock();
  if (victimized_size == 0) {
    lock.unlock();
    return false;
  }

  while (true) {
    if (clock_pointer >= static_cast<int>(pins_states.size())) {
      clock_pointer = 0;
    }
    if (!pins_states[clock_pointer]) {
      if (!access_states[clock_pointer]) {
        *frame_id = clock_pointer;
        pins_states[clock_pointer] = true;
        victimized_size--;
        clock_pointer++;

        lock.unlock();
        return true;
      } else {
        access_states[clock_pointer] = false;
        clock_pointer++;
      }
    } else {
      clock_pointer++;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  lock.lock();
  if (frame_id < static_cast<int>(pins_states.size())) {
    if (!pins_states[frame_id]) {
      pins_states[frame_id] = true;
      victimized_size--;
    }
  }
  lock.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  lock.lock();
  if (frame_id < static_cast<int>(pins_states.size())) {
    if (pins_states[frame_id]) {
      pins_states[frame_id] = false;
      victimized_size++;
    }
    access_states[frame_id] = true;
  }
  lock.unlock();
}

size_t ClockReplacer::Size() {
  lock.lock();
  int size = victimized_size;
  lock.unlock();
  return size;
}

}  // namespace bustub
