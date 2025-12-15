#pragma once

#include "duckdb/common/exception.hpp"
#include "pstsdk/mapitags.h"
#include "pstsdk/pst/pst.h"
#include <unordered_map>

namespace intellekt::duckpst::pst {

#define MESSAGE_CLASSES(LT)                                                                                            \
	LT(Appointment)                                                                                                    \
	LT(Contact)                                                                                                        \
	LT(Note)                                                                                                           \
	LT(StickyNote)                                                                                                     \
	LT(Task)

#define MESSAGE_CLASS_ENUM(name)    name,
#define CONTAINER_CLASS_NAME(name)  "IPF." #name,
#define MESSAGE_CLASS_NAME(name)    "IPM." #name,
#define MESSAGE_CLASS_ENTRY(name)   {MESSAGE_CLASS_NAME(name) name},
#define CONTAINER_CLASS_ENTRY(name) {CONTAINER_CLASS_NAME(name) name},

enum MessageClass { MESSAGE_CLASSES(MESSAGE_CLASS_ENUM) };

static const std::vector<const char *> MESSAGE_CLASS_NAMES = {MESSAGE_CLASSES(MESSAGE_CLASS_NAME)};
static const std::vector<const char *> CONTAINER_CLASS_NAMES = {MESSAGE_CLASSES(MESSAGE_CLASS_NAME)};

static const std::unordered_map<std::string, MessageClass> MESSAGE_CLASS_MAP = {MESSAGE_CLASSES(MESSAGE_CLASS_ENTRY)};
static const std::unordered_map<std::string, MessageClass> CONTAINER_CLASS_MAP = {
    MESSAGE_CLASSES(CONTAINER_CLASS_ENTRY)};

auto constexpr BASE_CLASS = MessageClass::Note;

/**
 * @brief Get the "IPM" string of the message class
 *
 * @param c
 * @return std::string
 */
inline std::string message_class_name(const MessageClass &c) {
	return MESSAGE_CLASS_NAMES[static_cast<int>(c)];
}

/**
 * @brief Get the "IPF" string of the folder's container class
 *
 * @param c
 * @return std::string
 */
inline std::string container_class_name(const MessageClass &c) {
	return CONTAINER_CLASS_NAMES[static_cast<int>(c)];
}

/**
 * @brief A wrapper for pstsdk::message so we can treat its prop bag differently
 *        for different schemas. Defaults to IPM.Note.
 *
 * @tparam V
 */
template <MessageClass V>
struct Message {
	pstsdk::pst &pst;
	pstsdk::message &message;

	inline Message(pstsdk::pst &pst, pstsdk::message &message) : pst(pst), message(message) {
		auto message_class = message.get_property_bag().read_prop_if_exists<std::string>(PR_MESSAGE_CLASS_A);
		if (!message_class)
			throw duckdb::InvalidInputException("Message is missing PR_MESSAGE_CLASS attribute");

		// IPM.Note is a base class, so it can be everything
		if constexpr (V == BASE_CLASS) {
			return;
		}

		auto templ_name = message_class_name(V);

		if (*message_class != message_class_name(V))
			throw duckdb::InvalidInputException("Message instantiated as %s, but is %s", templ_name, *message_class);
	}

	inline MessageClass message_class() {
		return V;
	}
};

inline MessageClass container_class(const pstsdk::folder &folder) {
	auto maybe_container_class = folder.get_property_bag().read_prop_if_exists<std::string>(PR_CONTAINER_CLASS_A);

	// TODO: iirc this is the outlook default
	auto klass = BASE_CLASS;

	if (maybe_container_class) {
		auto maybe_klass = CONTAINER_CLASS_MAP.find(*maybe_container_class);
		if (maybe_klass != CONTAINER_CLASS_MAP.end()) {
			auto &[_name, k] = *maybe_klass;
			klass = k;
		}
	}

	return klass;
}

inline MessageClass message_class(const pstsdk::message &msg) {
	auto maybe_msg_class = msg.get_property_bag().read_prop_if_exists<std::string>(PR_MESSAGE_CLASS_A);

	// TODO: iirc this is the outlook default
	auto klass = BASE_CLASS;

	if (maybe_msg_class) {
		auto maybe_klass = MESSAGE_CLASS_MAP.find(*maybe_msg_class);
		if (maybe_klass != MESSAGE_CLASS_MAP.end()) {
			auto &[_name, k] = *maybe_klass;
			klass = k;
		}
	}

	return klass;
}

} // namespace intellekt::duckpst::pst