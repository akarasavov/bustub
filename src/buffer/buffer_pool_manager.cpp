//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <include/buffer/clock_replacer.h>
#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  const auto frame_id_op = getFrameId(page_id);
  if (frame_id_op.has_value()) {
    replacer_->Pin(frame_id_op.value());
    return getPageByFrameId(frame_id_op.value());
  }
  frame_id_t frame_id = pickVictimPage();
  if (frame_id == -1) {
    return nullptr;
  }
  Page *page = getPageByFrameId(frame_id);

  page_table_.insert({page_id, frame_id});
  page->ResetMemory();
  replacer_->Pin(frame_id);

  page->page_id_ = page_id;
  page->pin_count_ = 1;  // do we need to pin it?
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());

  return page;

  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  std::optional<frame_id_t> frame_id = getFrameId(page_id);
  if (!frame_id.has_value()) {
    return false;
  }

  Page *page = getPageByFrameId(frame_id.value());
  if (page->GetPinCount() <= 0) {
    return false;
  }
  page->pin_count_--;
  page->is_dirty_ |= is_dirty;
  replacer_->Unpin(frame_id.value());
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  std::optional<frame_id_t> frame_id = getFrameId(page_id);
  if (!frame_id.has_value()) {
    return false;
  }
  Page *page = getPageByFrameId(frame_id.value());
  if (!page->IsDirty()) {
    LOG_DEBUG("Page with %d is not dirty", page->GetPageId());
    return false;
  }

  LOG_DEBUG("Page with id %d will be flushed to disk", page->GetPageId());
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  // Make sure you call DiskManager::WritePage!
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  if (isAllPined()) {
    return nullptr;
  }

  frame_id_t frame_id_t = pickVictimPage();
  if (frame_id_t < 0) {
    return nullptr;
  }

  Page *victim_page = getPageByFrameId(frame_id_t);
  page_id_t new_page_id = disk_manager_->AllocatePage();

  victim_page->ResetMemory();
  victim_page->page_id_ = new_page_id;
  victim_page->pin_count_ = 1;
  victim_page->is_dirty_ = false;

  page_table_.insert({new_page_id, frame_id_t});
  *page_id = victim_page->GetPageId();
  return victim_page;

  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  //  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  const auto frame_id = getFrameId(page_id);
  if (!frame_id.has_value()) {
    return true;
  }
  Page *page = getPageByFrameId(frame_id.value());
  if (page->GetPinCount() != 0) {
    return false;
  }

  page_table_.erase(page->page_id_);
  page->ResetMemory();
  free_list_.push_back(frame_id.value());

  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  for (const auto pair : page_table_) {
    FlushPageImpl(pair.first);
  }
  // You can do it!
}

}  // namespace bustub
