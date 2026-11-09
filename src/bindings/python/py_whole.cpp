#include "py_api.hpp"

#include "py_error.hpp"
#include "py_future.hpp"
#include "../../core/completion.hpp"
#include "../../core/config.hpp"
#include "../../core/global_thread_pool.hpp"
#include "../../platform/native_file.hpp"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

namespace ayafileio::python {
namespace {

struct ScopedReadHandle {
    platform::file_handle_t value;

    ~ScopedReadHandle() { platform::file_close(value); }
};

core::Completion read_failure(const std::string& path, const char* stage) {
    const auto error = platform::native_file_last_error();
    return core::Completion::failure(
        core::ReqType::Read,
        core::IoError::native(static_cast<std::uint32_t>(error),
                              std::string("Failed to ") + stage + " file: " + path));
}

// Open, stat, read and close in one worker task, without entering Python.
core::Completion read_whole_blocking(const std::string& path) {
    const auto handle = platform::file_open(path.c_str(), platform::kOpenRead);
    if (handle == platform::kFileInvalid) {
        return read_failure(path, "open");
    }
    const ScopedReadHandle owner{handle};

    unsigned long long total = 0;
    if (!platform::file_size(handle, &total)) {
        return read_failure(path, "stat");
    }
    if (total > static_cast<unsigned long long>(std::numeric_limits<Py_ssize_t>::max())) {
        return core::Completion::failure(
            core::ReqType::Read, core::IoError::runtime("File is too large to read: " + path));
    }
    if (total == 0) {
        return core::Completion::read({}, 0);
    }

    const auto size = static_cast<std::size_t>(total);
    auto buffer = core::Buffer::heap(size);
    std::size_t filled = 0;
    while (filled < size) {
        const auto got = platform::file_pread(
            handle, buffer.data() + filled, size - filled, static_cast<long long>(filled));
        if (got < 0) {
            return read_failure(path, "read");
        }
        if (got == 0) break;  // The file may have been truncated after stat.
        filled += static_cast<std::size_t>(got);
    }
    return core::Completion::read(std::move(buffer), filled);
}

} // namespace

nb::object py_read_whole_async(const char* path) {
    auto context = LoopContext::current();
    std::string path_copy(path);

    auto operation = std::make_shared<PyOperation>(context, core::ReqType::Read);
    operation->submitted();

    auto& pool = core::GlobalThreadPool::instance();
    pool.ensure_started(core::effective_io_worker_count());
    pool.enqueue([operation, path_copy = std::move(path_copy)]() {
        core::Completion result;
        try {
            result = read_whole_blocking(path_copy);
        } catch (const core::IoException& error) {
            result = core::Completion::failure(core::ReqType::Read, error.error());
        } catch (const std::exception& error) {
            result = core::Completion::failure(
                core::ReqType::Read, core::IoError::runtime(error.what()));
        }
        operation->complete(std::move(result));
    });
    return operation->future_object();
}

} // namespace ayafileio::python
