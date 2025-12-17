// SPDX-License-Identifier: BSD-3-Clause

use string_cache::{Atom, EmptyStaticAtomSet};

/// An interned string type that uses a static atom set.
pub type StringAtom = Atom<EmptyStaticAtomSet>;
