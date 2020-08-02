#!/usr/bin/env bash

node=$(hostname)
echo "node=${node},dc=dcx"

# echo "node=${node},dc=dcx"

# for i in `seq 0 1 40`; do
# node=node$i

# case $node in
#     node[1-4])
#         echo "node=${node},dc=dc1"
#         ;;
#     node[5-8])
#         echo "node=${node},dc=dc2"
#         ;;
#     node9|node1[0-2])
#         echo "node=${node},dc=dc3"
#         ;;
#     node1[3-6])
#         echo "node=${node},dc=dc4"
#         ;;
#     node1[7-9]|node20)
#         echo "node=${node},dc=dc5"
#         ;;
#     node2[1-4])
#         echo "node=${node},dc=dc6"
#         ;;
#     node2[5-8])
#         echo "node=${node},dc=dc7"
#         ;;
#     node29|node3[0-2])
#         echo "node=${node},dc=dc8"
#         ;;
#     node3[3-6])
#         echo "node=${node},dc=dc9"
#         ;;
#     node3[7-9]|node40)
#         echo "node=${node},dc=dc10"
#         ;;
#     *)
#         echo "node=${node},dc=dcx"
#         ;;
# esac

# done

