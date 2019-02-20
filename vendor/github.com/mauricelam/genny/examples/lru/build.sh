#!/bin/bash
cat ./lru_generic.go | ../../genny gen "Key=string CachedValue=int"
