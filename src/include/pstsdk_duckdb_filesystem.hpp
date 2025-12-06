#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "pstsdk/util/primitives.h"
#include "pstsdk/util/util.h"
#include "duckdb/common/open_file_info.hpp"

namespace intellekt::duckpst {

/**
 * @brief pstsdk file implementation for duckdb::FileHandle
 */
class dfile : public pstsdk::file {
	duckdb::unique_ptr<duckdb::FileHandle> file_handle;

public:
	/**
	 * @brief Construct a new "dfile"
	 *
	 * @param fs DuckDB filesystem
	 * @param file DuckDB file info
	 */
	dfile(duckdb::FileSystem &fs, const duckdb::OpenFileInfo &file);

	static std::shared_ptr<pstsdk::file> open(duckdb::FileSystem &fs, const duckdb::OpenFileInfo &finfo);

	size_t read(std::vector<pstsdk::byte> &buffer, pstsdk::ulonglong offset) const override;
	size_t write(const std::vector<pstsdk::byte> &buffer, pstsdk::ulonglong offset) override;
};
} // namespace intellekt::duckpst
