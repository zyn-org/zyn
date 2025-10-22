// SPDX-License-Identifier: AGPL-3.0

use string_cache::{Atom, EmptyStaticAtomSet};

/// An interned string type that uses a static atom set.
pub type StringAtom = Atom<EmptyStaticAtomSet>;
