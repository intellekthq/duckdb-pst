#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "pst/duckdb_filesystem.hpp"
#include "pstsdk/util/util.h"

#include <memory>

namespace intellekt::duckpst::pst {
using namespace duckdb;

dfile::dfile(ClientContext &ctx, const OpenFileInfo &file, bool writable)
    : pstsdk::file() {
  auto &fs = FileSystem::GetFileSystem(ctx);
  // Read plus write is O_RDWR with no truncation, which is what editing a store
  // in place needs. No lock is taken, so the read handles a scan still holds
  // stay valid alongside it
  auto flags = writable ? FileOpenFlags::FILE_FLAGS_READ |
                              FileOpenFlags::FILE_FLAGS_WRITE
                        : FileOpenFlags::FILE_FLAGS_READ;
  file_handle = fs.OpenFile(file, flags);
}

size_t dfile::read(std::vector<pstsdk::byte> &buffer,
                   pstsdk::ulonglong offset) const {
  idx_t read_size = buffer.size();

  // pstsdk::file documents out_of_range past EOF, and
  // db_writer::looks_like_page catches it to reject a candidate page address.
  // An IOException there aborts a whole wipe instead
  if (offset + read_size > (pstsdk::ulonglong)file_handle->GetFileSize())
    throw std::out_of_range("dfile::read past end of file");

  file_handle->Read(&buffer.data()[0], read_size, offset);
  return read_size;
}

size_t dfile::write(const std::vector<pstsdk::byte> &buffer,
                    pstsdk::ulonglong offset) {
  idx_t write_size = buffer.size();
  // Positional, like read. One handle is shared by every per-thread copy of a
  // pst, so seeking it moves the cursor out from under whoever else holds it
  file_handle->Write(
      QueryContext(),
      const_cast<void *>(reinterpret_cast<const void *>(&buffer.data()[0])),
      write_size, offset);
  return write_size;
}

std::shared_ptr<pstsdk::file> dfile::open(duckdb::ClientContext &ctx,
                                          const duckdb::OpenFileInfo &finfo) {
  return std::make_shared<dfile>(ctx, finfo);
}

std::shared_ptr<dfile> dfile::open_writable(duckdb::ClientContext &ctx,
                                            const duckdb::OpenFileInfo &finfo) {
  return std::make_shared<dfile>(ctx, finfo, true);
}

void dfile::sync() { file_handle->Sync(); }

} // namespace intellekt::duckpst::pst
