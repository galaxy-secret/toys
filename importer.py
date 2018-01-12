# coding=utf-8
import struct
import os, sys
import math

def read_utf16_str (f, offset=-1, len=2):
    if offset >= 0:
        f.seek(offset)
    str = f.read(len)
    return str.decode('UTF-16LE')

def read_uint16 (f):
    return struct.unpack ('<H', f.read(2))[0]

def get_word_from_sogou_cell_dict (fname):
    f = open (fname, 'rb')
    file_size = os.path.getsize(fname)

    hz_offset = 0
    mask = struct.unpack('B', bytes([f.read(128)[4]]))[0]
    if mask == 0x44:
        hz_offset = 0x2628
    elif mask == 0x45:
        hz_offset = 0x26c4
    else:
        sys.exit(1)

    title   = read_utf16_str (f, 0x130, 0x338  - 0x130)
    type    = read_utf16_str (f, 0x338, 0x540  - 0x338)
    desc    = read_utf16_str (f, 0x540, 0xd40  - 0x540)
    samples = read_utf16_str (f, 0xd40, 0x1540 - 0xd40)

    py_map = {}
    f.seek(0x1540+4)

    while 1:
        py_code = read_uint16 (f)
        py_len  = read_uint16 (f)
        py_str  = read_utf16_str (f, -1, py_len)

        if py_code not in py_map:
            py_map[py_code] = py_str

        if py_str == 'zuo':
            break

    f.seek(hz_offset)
    while f.tell() != file_size:
        word_count   = read_uint16 (f)
        pinyin_count = read_uint16 (f) / 2

        py_set = []
        for i in range(math.ceil(pinyin_count)):
            py_id = read_uint16(f)
            py_set.append(py_map[py_id])
        py_str = "'".join (py_set)

        for i in range(word_count):
            word_len = read_uint16(f)
            word_str = read_utf16_str (f, -1, word_len)
            f.read(12) 
            yield py_str, word_str

    f.close()

def showtxt (records):
    f = open("words-temp.dic", 'a', encoding='utf8')
    for (pystr, utf8str) in records:
        f.write(utf8str + '\n')
        #print len(utf8str), utf8str
        print(utf8str)

    f.close()

def main ():
    # if len (sys.argv) != 2:
    #     print "Please specify the Sogou PinYin Cell dict file!"
    #     exit (1)
    # generator = get_word_from_sogou_cell_dict(sys.argv[1])
    # showtxt(generator)
    files = ["桂林市城市信息精选.scel",
             "海口市城市信息精选.scel",
             "海南景点.scel",
             "丽江市城市信息精选.scel",
             "旅游词汇大全【官方推荐】.scel",
             "旅游词库大全.scel",
             "三亚市城市信息精选.scel",
             "上海市城市信息精选.scel",
             "张家界市城市信息精选.scel",
             "中国风景名胜.scel"
             ]
    for item in files:
        generator = get_word_from_sogou_cell_dict (item)
        showtxt(generator)

if __name__ == "__main__":
    main()
