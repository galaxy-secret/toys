# coding=utf-8

import os
import sys

def main():
    words = set()
    with open("words-temp.dic", 'r', encoding='utf8') as f:
        for word in f.readlines():
            if word.strip():
                words.add(word.strip())

    with open("words-my.dic", 'w', encoding='utf8') as f:
        for word in words:
            if word.strip():
                print(word.strip())
                f.write(word.strip() + '\n')
        os.remove("words-temp.dic")

if __name__ == '__main__':
    main()
