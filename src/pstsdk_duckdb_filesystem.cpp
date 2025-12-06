#include "pstsdk_duckdb_filesystem.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "pstsdk/util/util.h"
#include <memory>

namespace intellekt::duckpst {
using namespace duckdb;

dfile::dfile(FileSystem &fs, const OpenFileInfo &file) : pstsdk::file() {
	file_handle = fs.OpenFile(file, FileOpenFlags::FILE_FLAGS_READ);
}

size_t dfile::read(std::vector<pstsdk::byte> &buffer, pstsdk::ulonglong offset) const {
	idx_t read_size = buffer.size();
	file_handle->Seek(offset);
	return file_handle->Read(&buffer.data()[0], read_size);
}

size_t dfile::write(const std::vector<pstsdk::byte> &buffer, pstsdk::ulonglong offset) {
	idx_t write_size = buffer.size();
	file_handle->Seek(offset);
	return file_handle->Write(const_cast<void *>(reinterpret_cast<const void *>(&buffer.data()[0])), write_size);
}

std::shared_ptr<pstsdk::file> dfile::open(duckdb::FileSystem &fs, const duckdb::OpenFileInfo &finfo) {
	return std::make_shared<dfile>(fs, finfo);
}

} // namespace intellekt::duckpst
