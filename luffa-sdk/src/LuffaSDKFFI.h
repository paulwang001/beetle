// This file was autogenerated by some hot garbage in the `uniffi` crate.
// Trust me, you don't want to mess with it!

#pragma once

#include <stdbool.h>
#include <stdint.h>

// The following structs are used to implement the lowest level
// of the FFI, and thus useful to multiple uniffied crates.
// We ensure they are declared exactly once, with a header guard, UNIFFI_SHARED_H.
#ifdef UNIFFI_SHARED_H
    // We also try to prevent mixing versions of shared uniffi header structs.
    // If you add anything to the #else block, you must increment the version suffix in UNIFFI_SHARED_HEADER_V4
    #ifndef UNIFFI_SHARED_HEADER_V4
        #error Combining helper code from multiple versions of uniffi is not supported
    #endif // ndef UNIFFI_SHARED_HEADER_V4
#else
#define UNIFFI_SHARED_H
#define UNIFFI_SHARED_HEADER_V4
// ⚠️ Attention: If you change this #else block (ending in `#endif // def UNIFFI_SHARED_H`) you *must* ⚠️
// ⚠️ increment the version suffix in all instances of UNIFFI_SHARED_HEADER_V4 in this file.           ⚠️

typedef struct RustBuffer
{
    int32_t capacity;
    int32_t len;
    uint8_t *_Nullable data;
} RustBuffer;

typedef int32_t (*ForeignCallback)(uint64_t, int32_t, RustBuffer, RustBuffer *_Nonnull);

typedef struct ForeignBytes
{
    int32_t len;
    const uint8_t *_Nullable data;
} ForeignBytes;

// Error definitions
typedef struct RustCallStatus {
    int8_t code;
    RustBuffer errorBuf;
} RustCallStatus;

// ⚠️ Attention: If you change this #else block (ending in `#endif // def UNIFFI_SHARED_H`) you *must* ⚠️
// ⚠️ increment the version suffix in all instances of UNIFFI_SHARED_HEADER_V4 in this file.           ⚠️
#endif // def UNIFFI_SHARED_H

void ffi_LuffaSDK_34a2_Client_object_free(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
void*_Nonnull LuffaSDK_34a2_Client_new(
      
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_show_code(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
uint64_t LuffaSDK_34a2_Client_contacts_offer(
      void*_Nonnull ptr,RustBuffer code,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_gen_offer_code(
      void*_Nonnull ptr,uint64_t did,
    RustCallStatus *_Nonnull out_status
    );
uint64_t LuffaSDK_34a2_Client_contacts_group_create(
      void*_Nonnull ptr,RustBuffer invitee,RustBuffer tag,
    RustCallStatus *_Nonnull out_status
    );
uint64_t LuffaSDK_34a2_Client_contacts_anwser(
      void*_Nonnull ptr,uint64_t to,uint64_t offer_id,RustBuffer secret_key,
    RustCallStatus *_Nonnull out_status
    );
uint64_t LuffaSDK_34a2_Client_send_msg(
      void*_Nonnull ptr,uint64_t to,RustBuffer msg,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_get_local_id(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_get_peer_id(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_get_did(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_relay_list(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_keys(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
int8_t LuffaSDK_34a2_Client_connect(
      void*_Nonnull ptr,RustBuffer peer_id,
    RustCallStatus *_Nonnull out_status
    );
void LuffaSDK_34a2_Client_init(
      void*_Nonnull ptr,RustBuffer cfg_path,
    RustCallStatus *_Nonnull out_status
    );
uint64_t LuffaSDK_34a2_Client_start(
      void*_Nonnull ptr,RustBuffer key,RustBuffer tag,uint64_t cb,
    RustCallStatus *_Nonnull out_status
    );
void LuffaSDK_34a2_Client_stop(
      void*_Nonnull ptr,
    RustCallStatus *_Nonnull out_status
    );
void LuffaSDK_34a2_Client_save_session(
      void*_Nonnull ptr,uint64_t did,RustBuffer tag,RustBuffer read,RustBuffer reach,RustBuffer msg,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_session_list(
      void*_Nonnull ptr,uint32_t top,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_session_page(
      void*_Nonnull ptr,uint32_t page,uint32_t size,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_contacts_list(
      void*_Nonnull ptr,uint8_t c_type,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_search(
      void*_Nonnull ptr,RustBuffer query,uint32_t offet,uint32_t limit,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_recent_messages(
      void*_Nonnull ptr,uint64_t did,uint32_t top,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_find_contacts_tag(
      void*_Nonnull ptr,uint64_t did,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_meta_msg(
      void*_Nonnull ptr,RustBuffer data,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_read_msg_with_meta(
      void*_Nonnull ptr,uint64_t did,uint64_t crc,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_gen_key(
      void*_Nonnull ptr,RustBuffer password,int8_t store,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_import_key(
      void*_Nonnull ptr,RustBuffer phrase,RustBuffer password,
    RustCallStatus *_Nonnull out_status
    );
int8_t LuffaSDK_34a2_Client_save_key(
      void*_Nonnull ptr,RustBuffer name,
    RustCallStatus *_Nonnull out_status
    );
int8_t LuffaSDK_34a2_Client_remove_key(
      void*_Nonnull ptr,RustBuffer name,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer LuffaSDK_34a2_Client_read_key_phrase(
      void*_Nonnull ptr,RustBuffer name,
    RustCallStatus *_Nonnull out_status
    );
void ffi_LuffaSDK_34a2_Callback_init_callback(
      ForeignCallback  _Nonnull callback_stub,
    RustCallStatus *_Nonnull out_status
    );
uint64_t LuffaSDK_34a2_public_key_to_id(
      RustBuffer public_key,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer ffi_LuffaSDK_34a2_rustbuffer_alloc(
      int32_t size,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer ffi_LuffaSDK_34a2_rustbuffer_from_bytes(
      ForeignBytes bytes,
    RustCallStatus *_Nonnull out_status
    );
void ffi_LuffaSDK_34a2_rustbuffer_free(
      RustBuffer buf,
    RustCallStatus *_Nonnull out_status
    );
RustBuffer ffi_LuffaSDK_34a2_rustbuffer_reserve(
      RustBuffer buf,int32_t additional,
    RustCallStatus *_Nonnull out_status
    );
