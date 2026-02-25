@echo off
start cmd /k "cd /d E:\_Projects\Website\Team Website\Backend-main && npm start"
start cmd /k "cd /d E:\_Projects\Website\Team Website\Front-main && npm run dev"
timeout /t 10
start http://localhost:5173/