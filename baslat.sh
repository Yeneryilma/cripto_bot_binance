#!/bin/bash
clear
echo "============================================="
echo "  KRIPTO PARA FUTURES PIYASA ANALIZ SISTEMI"
echo "============================================="
echo ""
echo "1- Backend sunucusunu baslat (Python API)"
echo "2- Frontend arayuzunu ac (index.html)"
echo "3- Tumunu baslat"
echo "4- Cikis"
echo ""
read -p "Seciminiz (1-4): " secim

DIR="$(cd "$(dirname "$0")" && pwd)"

case $secim in
    1)
        clear
        echo "Backend sunucusu baslatiliyor..."
        cd "$DIR"
        python3 backend.py &
        echo ""
        echo "Backend sunucusu http://localhost:5000 adresinde calisiyor"
        echo ""
        read -p "Devam icin Enter'a basin..."
        ;;
    2)
        clear
        echo "Frontend arayuzu aciliyor..."
        xdg-open "$DIR/index.html" 2>/dev/null || gnome-open "$DIR/index.html" 2>/dev/null || echo "Tarayici acilamadi. index.html dosyasini manuel acin."
        ;;
    3)
        clear
        echo "Backend ve Frontend baslatiliyor..."
        cd "$DIR"
        python3 backend.py &
        sleep 3
        xdg-open "$DIR/index.html" 2>/dev/null || gnome-open "$DIR/index.html" 2>/dev/null
        echo ""
        echo "Backend: http://localhost:5000"
        echo "Frontend: index.html (tarayicida acildi)"
        echo ""
        read -p "Devam icin Enter'a basin..."
        ;;
    4)
        exit 0
        ;;
    *)
        echo "Gecersiz secim"
        ;;
esac
