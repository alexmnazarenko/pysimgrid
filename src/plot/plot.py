# A. Nazarenko 2016
# -*- coding: utf-8 -*-

import argparse

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("mode", type=str, choices=["gantt"], help="plotting mode. defines both output and required input.")
  parser.add_argument("input", type=str, help="input file.")
  config = parser.parse_args()

if __name__ == '__main__':
  main()
