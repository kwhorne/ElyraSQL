#!/usr/bin/env bash

# Linker for the instrumented PGO build on macOS: links as rustc asks, without
# `-dead_strip`.
#
# ld64's dead stripping drops the profile records (`__llvm_prf_data`) of
# functions whose own bodies did not survive LTO -- inlined everywhere, or
# never called -- though the release build still compiles those functions
# and looks their profiles up. About 5,000 records went missing that way, and
# 191 functions of the release build, `Storage::apply_validated_batch` and
# `QualifierNormalizer::post_visit_query` among them, were optimized without
# profile data. Linux keeps the records (0 missing).
#
# Only the instrumented binary is linked this way; it is used for training
# and thrown away. The linker is not part of any symbol name, so the profile
# still matches the release build.

args=()
for arg in "$@"; do
  case "$arg" in
    -Wl,-dead_strip | -dead_strip) ;;
    *) args+=("$arg") ;;
  esac
done
exec cc "${args[@]}"
