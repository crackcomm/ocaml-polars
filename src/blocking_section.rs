// Copyright © 2024 Łukasz Kurowski. All rights reserved.
// SPDX-License-Identifier: MIT

extern "C" {
    pub fn caml_enter_blocking_section();
    pub fn caml_leave_blocking_section();
}

/// This module provides a way to run a closure in a section with released OCaml runtime.
/// On drop, this struct will acquire the OCaml runtime.
#[derive(Default)]
pub struct OCamlBlockingSection {}

impl OCamlBlockingSection {
    pub fn perform<T, F>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        unsafe { caml_enter_blocking_section() };
        f()
    }
}

impl Drop for OCamlBlockingSection {
    fn drop(&mut self) {
        unsafe { caml_leave_blocking_section() };
    }
}

/// Run the given closure in a section with released OCaml runtime.
pub fn releasing_runtime<T, F>(f: F) -> T
where
    F: FnOnce() -> T,
{
    OCamlBlockingSection::default().perform(f)
}
