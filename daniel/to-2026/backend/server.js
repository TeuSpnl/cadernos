const express = require("express");
const cors = require("cors");
const rotas = require("./routes");

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// Prefixo da API: /to-2026/api/*
app.use("/to-2026/api", rotas);

app.listen(PORT, () => {
  console.log(`API rodando em http://localhost:${PORT}/to-2026/api`);
});
