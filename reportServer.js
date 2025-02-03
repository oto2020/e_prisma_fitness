const express = require('express');
const cors = require('cors');
const { PrismaClient } = require('@prisma/client');

const app = express();
const prisma = new PrismaClient();

app.use(express.json());
app.use(cors());

app.post('/trainers_conversation_for_month', async (req, res) => {
    try {
        const {
            year,
            month,
            serviceName,
            saleDivisions,
            conversationDays
        } = req.body;

        if (!year || !month || !serviceName || !saleDivisions.length || !conversationDays) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        const placeholders = saleDivisions.map(() => '?').join(', '); // Формируем ?, ?, ?, ... в зависимости от длины массива

        const query = `
            WITH ServiceCounts AS (
                SELECT 
                    s.trainer,
                    COUNT(*) AS sessions_count
                FROM services s
                WHERE YEAR(s.datetime) = ?
                  AND MONTH(s.datetime) = ?
                  AND s.name = ?
                GROUP BY s.trainer
            ),
            FirstSales AS (
                SELECT 
                    s.trainer,
                    s.client,
                    MIN(sa.datetime) AS first_sale_date,
                    MIN(sa.id) AS first_sale_id
                FROM services s
                JOIN sales sa 
                    ON s.client = sa.client 
                    AND s.trainer = sa.trainer
                    AND sa.division IN (${placeholders}) 
                    AND sa.datetime >= s.datetime
                    AND sa.datetime <= DATE_ADD(s.datetime, INTERVAL ? DAY)
                    AND sa.final_price > 0
                WHERE YEAR(s.datetime) = ?
                  AND MONTH(s.datetime) = ?
                  AND s.name = ?
                GROUP BY s.trainer, s.client
            ),
            FirstSalesSummary AS (
                SELECT 
                    fs.trainer,
                    COUNT(fs.client) AS first_sales_count,
                    COALESCE(SUM(sa.final_price), 0) AS total_first_sales_amount
                FROM FirstSales fs
                JOIN sales sa ON fs.first_sale_id = sa.id
                WHERE sa.final_price > 0
                GROUP BY fs.trainer
            )
            SELECT 
                COALESCE(sc.trainer, fs.trainer) AS trainer,
                COALESCE(sc.sessions_count, 0) AS sessions_count,
                COALESCE(fs.first_sales_count, 0) AS first_sales_count,
                COALESCE(fs.total_first_sales_amount, 0) AS total_first_sales_amount
            FROM ServiceCounts sc
            LEFT JOIN FirstSalesSummary fs ON sc.trainer = fs.trainer

            UNION

            SELECT 
                COALESCE(sc.trainer, fs.trainer) AS trainer,
                COALESCE(sc.sessions_count, 0) AS sessions_count,
                COALESCE(fs.first_sales_count, 0) AS first_sales_count,
                COALESCE(fs.total_first_sales_amount, 0) AS total_first_sales_amount
            FROM FirstSalesSummary fs
            LEFT JOIN ServiceCounts sc ON sc.trainer = fs.trainer

            ORDER BY trainer;
        `;

        const params = [
            year, 
            month, 
            serviceName, 
            ...saleDivisions, // Передаём все значения из saleDivisions
            conversationDays, 
            year, 
            month, 
            serviceName
        ];

        const result = await prisma.$queryRawUnsafe(query, ...params);

        const serializedResult = result.map(row =>
            Object.fromEntries(
                Object.entries(row).map(([key, value]) => [
                    key,
                    typeof value === "bigint" ? value.toString() : value
                ])
            )
        );
        
        res.json(serializedResult);
        
    } catch (error) {
        console.error("Ошибка выполнения запроса:", error);
        res.status(500).json({ error: "Ошибка выполнения запроса" });
    } finally {
        await prisma.$disconnect();
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
