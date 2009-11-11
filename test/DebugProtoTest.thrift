/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace cpp thrift.test.debug
namespace java thrift.test

struct Doubles {
 1: double nan,
 2: double inf,
 3: double neginf,
 4: double repeating,
 5: double big,
 6: double small,
 7: double zero,
 8: double negzero,
}

struct OneOfEach {
  1: bool im_true,
  2: bool im_false,
  3: byte a_bite = 200,
  4: i16 integer16 = 33000,
  5: i32 integer32,
  6: i64 integer64 = 10000000000,
  7: double double_precision,
  8: string some_characters,
  9: string zomg_unicode,
  10: bool what_who,
  11: binary base64,
  12: list<byte> byte_list = [1, 2, 3],
  13: list<i16> i16_list = [1,2,3],
  14: list<i64> i64_list = [1,2,3]
}

struct Bonk {
  1: i32 type,
  2: string message,
}

struct Nesting {
  1: Bonk my_bonk,
  2: OneOfEach my_ooe,
}

struct HolyMoley {
  1: list<OneOfEach> big,
  2: set<list<string>> contain,
  3: map<string,list<Bonk>> bonks,
}

struct Backwards {
  2: i32 first_tag2,
  1: i32 second_tag1,
}

struct Empty {
}

struct Wrapper {
  1: Empty foo
}

struct RandomStuff {
  1: i32 a,
  2: i32 b,
  3: i32 c,
  4: i32 d,
  5: list<i32> myintlist,
  6: map<i32,Wrapper> maps,
  7: i64 bigint,
  8: double triple,
}

struct Base64 {
  1: i32 a,
  2: binary b1,
  3: binary b2,
  4: binary b3,
  5: binary b4,
  6: binary b5,
  7: binary b6,
}

struct CompactProtoTestStruct {
  // primitive fields
  1: byte   a_byte;
  2: i16    a_i16;
  3: i32    a_i32;
  4: i64    a_i64;
  5: double a_double;
  6: string a_string;
  7: binary a_binary;
  8: bool   true_field;
  9: bool   false_field;
  10: Empty empty_struct_field;
  
  // primitives in lists
  11: list<byte>    byte_list;
  12: list<i16>     i16_list;
  13: list<i32>     i32_list;
  14: list<i64>     i64_list;
  15: list<double>  double_list;
  16: list<string>  string_list;
  17: list<binary>  binary_list;
  18: list<bool>    boolean_list;
  19: list<Empty>   struct_list;
  
  // primitives in sets
  20: set<byte>     byte_set;
  21: set<i16>      i16_set;
  22: set<i32>      i32_set;
  23: set<i64>      i64_set;
  24: set<double>   double_set;
  25: set<string>   string_set;
  26: set<binary>   binary_set;
  27: set<bool>     boolean_set;
  28: set<Empty>    struct_set;
  
  // maps
  // primitives as keys
  29: map<byte, byte>             byte_byte_map;
  30: map<i16, byte>              i16_byte_map;
  31: map<i32, byte>              i32_byte_map;
  32: map<i64, byte>              i64_byte_map;
  33: map<double, byte>           double_byte_map;
  34: map<string, byte>           string_byte_map;
  35: map<binary, byte>           binary_byte_map;
  36: map<bool, byte>             boolean_byte_map;
  // primitives as values
  37: map<byte, i16>              byte_i16_map;
  38: map<byte, i32>              byte_i32_map;
  39: map<byte, i64>              byte_i64_map;
  40: map<byte, double>           byte_double_map;
  41: map<byte, string>           byte_string_map;
  42: map<byte, binary>           byte_binary_map;
  43: map<byte, bool>             byte_boolean_map;
  // collections as keys
  44: map<list<byte>, byte>       list_byte_map;
  45: map<set<byte>, byte>        set_byte_map;
  46: map<map<byte,byte>, byte>   map_byte_map;
  // collections as values
  47: map<byte, map<byte,byte>>   byte_map_map;
  48: map<byte, set<byte>>        byte_set_map;
  49: map<byte, list<byte>>       byte_list_map;
}


const CompactProtoTestStruct COMPACT_TEST = {
  'a_byte'             : 127,
  'a_i16'              : 32000,
  'a_i32'              : 1000000000,
  'a_i64'              : 0xffffffffff,
  'a_double'           : 5.6789,
  'a_string'           : "my string",
//'a_binary,'
  'true_field'         : 1,
  'false_field'        : 0,
  'empty_struct_field' : {},
  'byte_list'          : [-127, -1, 0, 1, 127],
  'i16_list'           : [-1, 0, 1, 0x7fff],
  'i32_list'           : [-1, 0, 0xff, 0xffff, 0xffffff, 0x7fffffff],
  'i64_list'           : [-1, 0, 0xff, 0xffff, 0xffffff, 0xffffffff, 0xffffffffff, 0xffffffffffff, 0xffffffffffffff, 0x7fffffffffffffff],
  'double_list'        : [0.1, 0.2, 0.3],
  'string_list'        : ["first", "second", "third"],
//'binary_list,'
  'boolean_list'       : [1, 1, 1, 0, 0, 0],
  'struct_list'        : [{}, {}],
  'byte_set'           : [-127, -1, 0, 1, 127],
  'i16_set'            : [-1, 0, 1, 0x7fff],
  'i32_set'            : [1, 2, 3],
  'i64_set'            : [-1, 0, 0xff, 0xffff, 0xffffff, 0xffffffff, 0xffffffffff, 0xffffffffffff, 0xffffffffffffff, 0x7fffffffffffffff],
  'double_set'         : [0.1, 0.2, 0.3],
  'string_set'         : ["first", "second", "third"],
//'binary_set,'
  'boolean_set'        : [1, 0],
  'struct_set'         : [{}],
  'byte_byte_map'      : {1 : 2},
  'i16_byte_map'       : {1 : 1, -1 : 1, 0x7fff : 1},
  'i32_byte_map'       : {1 : 1, -1 : 1, 0x7fffffff : 1},
  'i64_byte_map'       : {0 : 1,  1 : 1, -1 : 1, 0x7fffffffffffffff : 1},
  'double_byte_map'    : {-1.1 : 1, 1.1 : 1},
  'string_byte_map'    : {"first" : 1, "second" : 2, "third" : 3, "" : 0},
//'binary_byte_map,'
  'boolean_byte_map'   : {1 : 1, 0 : 0},
  'byte_i16_map'       : {1 : 1, 2 : -1, 3 : 0x7fff},
  'byte_i32_map'       : {1 : 1, 2 : -1, 3 : 0x7fffffff},
  'byte_i64_map'       : {1 : 1, 2 : -1, 3 : 0x7fffffffffffffff},
  'byte_double_map'    : {1 : 0.1, 2 : -0.1, 3 : 1000000.1},
  'byte_string_map'    : {1 : "", 2 : "blah", 3 : "loooooooooooooong string"},
//'byte_binary_map,'
  'byte_boolean_map'   : {1 : 1, 2 : 0},
  'list_byte_map'      : {[1, 2, 3] : 1, [0, 1] : 2, [] : 0},
  'set_byte_map'       : {[1, 2, 3] : 1, [0, 1] : 2, [] : 0},
  'map_byte_map'       : {{1 : 1} : 1, {2 : 2} : 2, {} : 0},
  'byte_map_map'       : {0 : {}, 1 : {1 : 1}, 2 : {1 : 1, 2 : 2}},
  'byte_set_map'       : {0 : [], 1 : [1], 2 : [1, 2]},
  'byte_list_map'      : {0 : [], 1 : [1], 2 : [1, 2]},
}



service Srv {
  i32 Janky(1: i32 arg);
  
  // return type only methods
  
  void voidMethod();
  i32 primitiveMethod();
  CompactProtoTestStruct structMethod();
}

service Inherited extends Srv {
  i32 identity(1: i32 arg)
}

service EmptyService {}

// The only purpose of this thing is to increase the size of the generated code
// so that ZlibTest has more highly compressible data to play with.
struct BlowUp {
  1: map<list<i32>,set<map<i32,string>>> b1;
  2: map<list<i32>,set<map<i32,string>>> b2;
  3: map<list<i32>,set<map<i32,string>>> b3;
  4: map<list<i32>,set<map<i32,string>>> b4;
}


struct ReverseOrderStruct {
  4: string first;
  3: i16 second;
  2: i32 third;
  1: i64 fourth;
}

service ReverseOrderService {
  void myMethod(4: string first, 3: i16 second, 2: i32 third, 1: i64 fourth);
}

union TestUnion {
  /**
   * A doc string
   */
  1: string string_field;
  2: i32 i32_field;
  3: OneOfEach struct_field;
  4: list<RandomStuff> struct_list;
  5: i32 other_i32_field;
}

union ComparableUnion {
  1: string string_field;
  2: binary binary_field;
}

struct StructWithAUnion {
  1: TestUnion test_union;
}

struct PrimitiveThenStruct {
  1: i32 blah;
  2: i32 blah2;
  3: Backwards bw;
}

typedef map<i32,i32> SomeMap

struct StructWithASomemap {
  1: required SomeMap somemap_field;
}