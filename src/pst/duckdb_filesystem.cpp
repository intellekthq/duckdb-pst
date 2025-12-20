#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "pst/duckdb_filesystem.hpp"
#include "pstsdk/util/util.h"

#include <memory>

namespace intellekt::duckpst::pst {
using namespace duckdb;

dfile::dfile(ClientContext &ctx, const OpenFileInfo &file) : pstsdk::file() {
  auto &fs = FileSystem::GetFileSystem(ctx);
  file_handle = fs.OpenFile(file, FileOpenFlags::FILE_FLAGS_READ);
}

size_t dfile::read(std::vector<pstsdk::byte> &buffer,
                   pstsdk::ulonglong offset) const {
  idx_t read_size = buffer.size();
  file_handle->Read(&buffer.data()[0], read_size, offset);
  return read_size;
}

size_t dfile::write(const std::vector<pstsdk::byte> &buffer,
                    pstsdk::ulonglong offset) {
  idx_t write_size = buffer.size();
  file_handle->Seek(offset);
  return file_handle->Write(
      const_cast<void *>(reinterpret_cast<const void *>(&buffer.data()[0])),
      write_size);
}

std::shared_ptr<pstsdk::file> dfile::open(duckdb::ClientContext &ctx,
                                          const duckdb::OpenFileInfo &finfo) {
  return std::make_shared<dfile>(ctx, finfo);
}

} // namespace intellekt::duckpst::pst
