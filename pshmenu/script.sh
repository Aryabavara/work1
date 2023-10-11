#!/bin/bash
 
# clear the screen
tput clear
 
# Move cursor to screen location X,Y (top left is 0,0)
tput cup 3 15
 
# Set a foreground colour using ANSI escape
tput setaf 3
tput sgr0
 
tput cup 5 17
# Set reverse video mode
tput rev
echo "PSH - MENU"
tput sgr0
 
tput cup 7 15
echo "1. User Management"
tput cup 8 15
echo "2. login"
tput cup 9 15
echo "3. exit"
# Set bold mode
tput bold
tput cup 11 15
read -p "Enter your choice [1-3] " choice
 
./hello.sh

