const { PrismaClient } = require('@prisma/client');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const prisma = new PrismaClient();
let isProcessing = false; // Флаг, чтобы предотвратить повторные запуски

async function loadPackagesData(filePath) {
  const workbook = xlsx.readFile(filePath);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  const jsonData = xlsx.utils.sheet_to_json(worksheet, { range: 2 }); // игнорируем первые две строки

  console.log(jsonData);
  
//   console.log(jsonData);
//   return;

  // Мапим данные к структуре нашей модели Package
  const packagesData = jsonData.map(row => ({
    id: String(row.id),
    dateSale: row.dateSale ? new Date(row.dateSale) : null,
    dateActivation: row.dateActivation ? new Date(row.dateActivation) : null,
    dateClosing: row.dateClosing ? new Date(row.dateClosing) : null,
    name: row.name,
    division: row.division,
    comment: row.comment,
    client: row.client,
    clientBirthday: row.clientBirthday ? new Date(row.clientBirthday) : null,
    clientPhone: row.clientPhone,
    trainer: row.trainer,
    price: row.price ? Number(row.price) : null,
    count: row.count ? parseInt(row.count): null,
    dateProdl: row.dateProdl ? new Date(row.dateProdl) : null,
    status: row.status
  }));

  console.log(`📄 Подготовлено ${packagesData.length} записей для загрузки в packages.`);

  try {
    // Удаляем все старые записи перед вставкой новых
    await prisma.package.deleteMany({});
    console.log("✅ Все старые данные из 'packages' удалены.");

    // Для больших объёмов данных используем пакетную вставку
    const batchSize = 500;
    for (let i = 0; i < packagesData.length; i += batchSize) {
      const batch = packagesData.slice(i, i + batchSize);
      console.log(`📤 Вставка записей: ${i + 1}-${i + batch.length} / ${packagesData.length}`);

      // Создаём записи
      await prisma.package.createMany({
        data: batch,
        skipDuplicates: true,
      });
    }

    console.log(`✅ Файл ${filePath} загружен в 'packages'!`);

    // Удаляем обработанный файл
    fs.unlink(filePath, (err) => {
      if (err) {
        console.error(`❌ Ошибка удаления файла ${filePath}:`, err);
      } else {
        console.log(`🗑️ Файл ${filePath} успешно удален.`);
      }
    });

  } catch (error) {
    console.error("❌ Ошибка при загрузке данных в 'packages':", error);
  }
}

async function processPackageFiles() {
  if (isProcessing) {
    console.log("⏳ Обработка уже выполняется, ждём следующего запуска...");
    return;
  }

  isProcessing = true; // Устанавливаем флаг обработки

  try {
    const directoryPath = path.join(__dirname, "..", process.env.FTP_FOLDER);
    const files = await fs.promises.readdir(directoryPath);
    
    // Ищем файлы с "ftp.packages" в названии
    const excelFiles = files.filter(file => file.startsWith('ftp.packages') && file.endsWith('.xlsx'));

    if (excelFiles.length === 0) {
      console.log("📂 Файлы для загрузки не найдены (packages).");
      return;
    }

    console.log(`🔍 Найдено ${excelFiles.length} файлов для обработки (packages).`);

    // Обрабатываем все найденные файлы последовательно
    for (const file of excelFiles) {
      await loadPackagesData(path.join(directoryPath, file));
    }

    console.log('🎉 Все файлы (packages) обработаны.');
  } catch (error) {
    console.error("❌ Ошибка при обработке файлов (packages):", error);
  } finally {
    isProcessing = false; // Сбрасываем флаг
  }
}

// Запускаем при старте
(async () => {
  console.log("🚀 Первоначальный запуск обработки файлов (packages)...");
  await processPackageFiles();
})();

// Запуск кода каждую минуту
setInterval(processPackageFiles, 60 * 1000);

// Обработчик завершения
process.on('SIGINT', async () => {
  console.log("\n🛑 Завершаем работу...");
  await prisma.$disconnect();
  process.exit();
});
