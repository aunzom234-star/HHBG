SecretVoice relay - ชุดสำหรับขึ้น Render

1) สร้าง repo ใหม่บน GitHub แล้วอัปโหลดไฟล์ทั้ง 4 ไฟล์นี้ (server.js, package.json, render.yaml, .gitignore)
   ไฟล์ต้องอยู่ที่รากของ repo ไม่ใช่ในโฟลเดอร์ย่อย
2) เข้า render.com -> New -> Blueprint -> เลือก repo นี้
3) ช่อง VC_PASSWORD ใส่รหัสผ่านที่ต้องการ (จำไว้) -> Apply
4) รอ deploy เสร็จ จะได้ลิงก์ https://ชื่อ.onrender.com
   ที่อยู่ relay ที่ใช้ในมอดคือ wss://ชื่อ.onrender.com (เปลี่ยน https เป็น wss)
5) เปิดลิงก์ในเบราว์เซอร์ ถ้าขึ้น "SecretVoice relay is running" แปลว่าใช้ได้
