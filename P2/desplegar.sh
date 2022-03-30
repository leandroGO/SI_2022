#!/bin/bash
for i in P1-base P1-ejb-servidor-remoto P1-ejb-cliente-remoto P1-ws; do
    cd $i
    ant limpiar-todo todo
    cd -
done
