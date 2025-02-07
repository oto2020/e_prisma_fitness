const { PrismaClient } = require('@prisma/client');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const prisma = new PrismaClient();
let isProcessing = false; // Флаг для предотвращения одновременных запусков

async function loadServicesData(filePath) {
  const workbook = xlsx.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  let jsonData = xlsx.utils.sheet_to_json(worksheet);

  jsonData = jsonData.slice(1, -1); // Удаляем первую и последнюю строку

  const servicesData = jsonData.map(row => ({
    id: String(row.id),
    name: row.name,
    division: row.division,
    trainer: row.trainer,
    client: row.client,
    basis: row.basis || null,
    comment: row.comment,
    datetime: new Date(row.datetime),
    price: Number(row.price)
  }));

  console.log(`📄 Подготовлено ${servicesData.length} записей.`);

  try {
    // Очистка таблицы перед загрузкой новых данных
    await prisma.service.deleteMany({});
    console.log("✅ Все старые данные удалены.");

    const batchSize = 500;
    for (let i = 0; i < servicesData.length; i += batchSize) {
      const batch = servicesData.slice(i, i + batchSize);
      console.log(`📤 Вставка записей: ${i + 1}-${i + batch.length} / ${servicesData.length}`);

      await prisma.service.createMany({
        data: batch,
        skipDuplicates: true
      });
    }

    console.log(`✅ Файл ${filePath} загружен.`);

    // Удаление файла после успешной загрузки
    fs.unlink(filePath, (err) => {
      if (err) {
        console.error(`❌ Ошибка удаления файла ${filePath}:`, err);
      } else {
        console.log(`🗑️ Файл ${filePath} успешно удален.`);
      }
    });

  } catch (error) {
    console.error("❌ Ошибка при загрузке данных:", error);
  }
}

async function processServiceFiles() {
  if (isProcessing) {
    console.log("⏳ Обработка уже выполняется, ждем следующего запуска...");
    return;
  }

  isProcessing = true; // Устанавливаем флаг обработки

  try {
    const directoryPath = path.join(__dirname, "..", process.env.FTP_FOLDER);
    const files = await fs.promises.readdir(directoryPath);
    
    const excelFiles = files.filter(file => file.startsWith('ftp.services') && file.endsWith('.xlsx'));

    if (excelFiles.length === 0) {
      console.log("📂 Файлы для загрузки не найдены.");
      return;
    }

    console.log(`🔍 Найдено ${excelFiles.length} файлов для обработки.`);

    for (const file of excelFiles) {
      await loadServicesData(path.join(directoryPath, file));
    }

    console.log('🎉 Все файлы обработаны.');
  } catch (error) {
    console.error("❌ Ошибка обработки файлов:", error);
  } finally {
    isProcessing = false; // Сбрасываем флаг после завершения обработки
  }
}

// 🔹 Запускаем сразу при запуске скрипта
(async () => {
  console.log("🚀 Первоначальный запуск обработки файлов...");
  await processServiceFiles();
})();

// 🔹 Запуск кода каждую минуту
setInterval(processServiceFiles, 60 * 1000);

// 🔹 Обработчик завершения
process.on('SIGINT', async () => {
  console.log("\n🛑 Завершаем работу...");
  await prisma.$disconnect();
  process.exit();
});
