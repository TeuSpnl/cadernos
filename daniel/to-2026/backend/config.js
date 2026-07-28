/**
 * Valor padrão inicial de INSCRICOES_ABERTAS (só na 1ª subida do banco).
 * Depois disso, o status fica no SQLite e muda pelo painel admin.
 *
 * true  → inscrição do evento → tabela inscricoes
 * false → formulário de interesse → tabela prospeccao
 */
module.exports = {
  INSCRICOES_ABERTAS: true,
};
