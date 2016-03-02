let () = Protobuf.(()) (* ocamldep *)

type m1 = {
  f1:  bool         [@key 1]  ;
  f2:  int          [@key 2]  [@encoding `varint];
  f3:  int          [@key 3]  [@encoding `zigzag];
  f4:  int          [@key 4]  [@encoding `bits32];
  f5:  int          [@key 5]  [@encoding `bits64];
  f6:  Int32.t      [@key 6]  [@encoding `varint];
  f7:  Int32.t      [@key 7]  [@encoding `zigzag];
  f8:  Int32.t      [@key 8]  [@encoding `bits32];
  f9:  Int32.t      [@key 9]  [@encoding `bits64];
  f10: Int64.t      [@key 10] [@encoding `varint];
  f11: Int64.t      [@key 11] [@encoding `zigzag];
  f12: Int64.t      [@key 12] [@encoding `bits32];
  f13: Int64.t      [@key 13] [@encoding `bits64];
  f14: Uint32.t     [@key 14] [@encoding `varint];
  f15: Uint32.t     [@key 15] [@encoding `zigzag];
  f16: Uint32.t     [@key 16] [@encoding `bits32];
  f17: Uint32.t     [@key 17] [@encoding `bits64];
  f18: Uint64.t     [@key 18] [@encoding `varint];
  f19: Uint64.t     [@key 19] [@encoding `zigzag];
  f20: Uint64.t     [@key 20] [@encoding `bits32];
  f21: Uint64.t     [@key 21] [@encoding `bits64];
  f22: float        [@key 22] [@encoding `bits32];
  f23: float        [@key 23] [@encoding `bits64];
  f24: string       [@key 24] ;
  f25: bytes        [@key 25] ;
  f26: int option   [@key 26] ;
  f27: int list     [@key 27] ;
  f28: int array    [@key 28] [@packed];
}
[@@deriving protobuf { protoc }]

type m2 =
| A                 [@key 1]
| B of int          [@key 2]
| C of (int * int)  [@key 3]
[@@deriving protobuf { protoc }]

type m3 =
| D [@key 1]
| E [@key 2]
[@@deriving protobuf { protoc }]

type m4 = {
  f1: m3            [@key 1] ;
  f2: m3            [@key 2] [@bare];
  f3: [`A [@key 1]] [@key 3] ;
  f4: [`A [@key 1]] [@key 4] [@bare];
}
[@@deriving protobuf { protoc }]

type m5 = {
  f1: bool          [@key 1] [@default true];
  f2: bool          [@key 2] [@default false];
  f3: int           [@key 3] [@default 42];
  f4: string        [@key 4] [@default "foo\"\nй"];
  f5: bytes         [@key 5] [@default Bytes.of_string "foo\"\nй"];
  f6: [`A [@key 1]] [@key 6] [@default `A] [@bare];
  f7: m3            [@key 7] [@default  D] [@bare];
}
[@@deriving protobuf { protoc }]

type m6 = Protoc_inner.foo
[@@deriving protobuf { protoc }]

module Mod = struct
  type foo = int [@@deriving protobuf { protoc }]
end

type m7 = Mod.foo
[@@deriving protobuf { protoc; protoc_import = ["Protoc_outer.Mod.protoc"] }]


