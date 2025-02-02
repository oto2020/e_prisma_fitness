const { PrismaClient } = require('@prisma/client');
const xlsx = require('xlsx');
const fs = require('fs');
const prisma = new PrismaClient();

async function main() {
  // Период оказания услуг
  const startDateString = '2024-06-01';
  const endDateString = '2025-12-10';
  console.log(endDateString);

  // Названия услуг
  const serviceName1 = '%тестирован%';
  const serviceName2 = '%в групповых%';
  const serviceName3 = '%в тренажерном%';
  const serviceName4 = '%в аква зоне%';

  const startDate = new Date(startDateString);
  const endDate = new Date(endDateString);

  const servicesSales = await prisma.$queryRaw`
    SELECT 
      s1.name AS serviceName1,
      s1.datetime AS serviceDatetime1,
      s1.trainer AS serviceTrainer1,
      s1.client AS serviceClient1,
      s2.name AS serviceName2,
      s2.datetime AS serviceDatetime2,
      s2.trainer AS serviceTrainer2,
      s2.client AS serviceClient2,
      IFNULL(DATEDIFF(s2.datetime, s1.datetime), 0) AS daysDifference
    FROM services AS s1
    LEFT JOIN services AS s2 ON s2.client = s1.client 
        AND s2.datetime > s1.datetime 
        AND (s2.name LIKE ${serviceName2} OR s2.name LIKE ${serviceName3} OR s2.name LIKE ${serviceName4})
    WHERE s1.name LIKE ${serviceName1}
      AND s1.datetime BETWEEN ${startDate} AND ${endDate}
    ORDER BY s1.datetime ASC, s2.datetime ASC;
  `;

  // Преобразование данных в формат для xlsx
  const data = servicesSales.map(row => ({
    serviceName1: row.serviceName1,
    serviceDatetime1: row.serviceDatetime1,
    serviceClient1: row.serviceClient1,
    serviceTrainer1: row.serviceTrainer1,
    daysDifference: Number(row.daysDifference), // Преобразуем в число
    serviceName2: row.serviceName2,
    serviceDatetime2: row.serviceDatetime2,
    serviceTrainer2: row.serviceTrainer2,
  }));

  // Создание новой книги
  const workbook = xlsx.utils.book_new();
  const worksheet = xlsx.utils.json_to_sheet(data);

  // Добавление листа в книгу
  xlsx.utils.book_append_sheet(workbook, worksheet, 'ServicesReport');

  // Сохранение файла
  const outputPath = `services_report ${startDateString} - ${endDateString}.xlsx`;
  xlsx.writeFile(workbook, outputPath);

  console.log(`Report saved to ${outputPath}`);
}

main()
  .catch(e => {
    console.error(e);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
