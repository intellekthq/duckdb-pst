#pragma once

#include "pstsdk/ltp/propbag.h"
#include "pstsdk/mapitags.h"
#include "pstsdk/pst/pst.h"
#include <type_traits>
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
 * @brief Get the container class of a folder by reading PR_CONTAINER_CLASS_A
 *
 * @param msg
 * @return MessageClass
 */
inline MessageClass container_class(const pstsdk::pst &pst, const pstsdk::node_id &nid) {
	auto bag = pstsdk::property_bag(pst.get_db().get()->lookup_node(nid));
	auto maybe_container_class = bag.read_prop_if_exists<std::string>(PR_CONTAINER_CLASS_A);

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

/**
 * @brief Get the container class of a message by reading PR_MESSAGE_CLASS_A
 *
 * @param msg
 * @return MessageClass
 */
inline MessageClass message_class(const pstsdk::pst &pst, const pstsdk::node_id &nid) {
	auto bag = pstsdk::property_bag(pst.get_db().get()->lookup_node(nid));
	auto maybe_msg_class = bag.read_prop_if_exists<std::string>(PR_MESSAGE_CLASS_A);

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

/**
 * @brief A typed wrapper for pstsdk prop bags, allowing them to be mounted directly from NID,
 *		  where the pstsdk companion object is instantiated alongside
 *
 * @tparam V The message/container class type
 * @tparam T The SDK companion object
 */
template <MessageClass V, typename T = pstsdk::message>
struct TypedBag {
	pstsdk::pst &pst;
	pstsdk::node node;
	pstsdk::property_bag bag;

	std::optional<T> sdk_object;

	inline TypedBag(pstsdk::pst &pst, pstsdk::node_id nid) : pst(pst), node(pst.get_db()->lookup_node(nid)), bag(node) {
#ifdef DUCKPST_TYPED_BAG_CHECK_STRICT
		auto message_class = message.get_property_bag().read_prop_if_exists<std::string>(PR_MESSAGE_CLASS_A);
		if (!message_class)
			throw duckdb::InvalidInputException("TypedBag is missing PR_MESSAGE_CLASS attribute");

		if constexpr (V == BASE_CLASS) {
			return;
		}

		auto templ_name = message_class_name(V);

		if (*message_class != message_class_name(V))
			throw duckdb::InvalidInputException("TypedBag instantiated as %s, but is %s", templ_name, *message_class);
#endif
		if constexpr (std::is_same_v<T, pstsdk::folder>) {
			sdk_object.emplace(pstsdk::folder(pst.get_db(), node));
		} else {
			sdk_object.emplace(pstsdk::message(node));
		}
	}

	inline MessageClass message_class() {
		return V;
	}
};

// bag constexpr helpers (used for serializer)

template <typename T>
struct is_folder_bag : std::false_type {};

template <MessageClass V>
struct is_folder_bag<TypedBag<V, pstsdk::folder>> : std::true_type {};

template <typename T>
inline constexpr bool is_folder_bag_v = is_folder_bag<T>::value;

} // namespace intellekt::duckpst::pst