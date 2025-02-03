const { PrismaClient } = require('@prisma/client');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const prisma = new PrismaClient();
let isProcessing = false; // Флаг, чтобы предотвратить повторные запуски

async function loadSalesData(filePath) {
  const workbook = xlsx.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  let jsonData = xlsx.utils.sheet_to_json(worksheet);

  jsonData = jsonData.slice(1, -1); // Удаляем первую и последнюю строку

  const salesData = jsonData.map(row => ({
    id: String(row.id),
    datetime: new Date(row.datetime),
    division: row.division,
    name: row.name,
    client: row.client,
    author: row.author,
    trainer: row.trainer,
    type: row.type,
    order_count: Number(row.order_count),
    order_price: row.order_price ? Number(row.order_price) : null,
    refund_count: row.refund_count ? Number(row.refund_count) : null,
    refund_price: row.refund_price ? Number(row.refund_price) : null,
    final_price: row.final_price ? Number(row.final_price) : null
  }));

  console.log(`📄 Подготовлено ${salesData.length} записей.`);

  try {
    await prisma.sale.deleteMany({});
    console.log("✅ Все старые данные удалены.");

    const batchSize = 500;
    for (let i = 0; i < salesData.length; i += batchSize) {
      const batch = salesData.slice(i, i + batchSize);
      console.log(`📤 Вставка записей: ${i + 1}-${i + batch.length} / ${salesData.length}`);

      await prisma.sale.createMany({
        data: batch,
        skipDuplicates: true
      });
    }

    console.log(`✅ Файл ${filePath} загружен.`);

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

async function processFiles() {
  if (isProcessing) {
    console.log("⏳ Обработка уже выполняется, ждем следующего запуска...");
    return;
  }

  isProcessing = true; // Устанавливаем флаг обработки

  try {
    const directoryPath = path.join(__dirname, "..", process.env.FTP_FOLDER);
    const files = await fs.promises.readdir(directoryPath);
    
    const excelFiles = files.filter(file => file.startsWith('ftp.sales') && file.endsWith('.xlsx'));

    if (excelFiles.length === 0) {
      console.log("📂 Файлы для загрузки не найдены.");
      return;
    }

    console.log(`🔍 Найдено ${excelFiles.length} файлов для обработки.`);

    for (const file of excelFiles) {
      await loadSalesData(path.join(directoryPath, file));
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
  await processFiles();
})();

// 🔹 Запуск кода каждую минуту
setInterval(processFiles, 60 * 1000);

// 🔹 Обработчик завершения
process.on('SIGINT', async () => {
  console.log("\n🛑 Завершаем работу...");
  await prisma.$disconnect();
  process.exit();
});
