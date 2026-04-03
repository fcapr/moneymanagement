# Configuration Documentation

## Parameters

### Financial Parameters
- `liquidita_31_giu_22`: Current liquidity as of 2026-01-03
  - Total: € #mettere i soldi attuali sui conti e togliere i guadagni del portafoglio
  - Note: Add remaining amounts from other liquid accounts

### Tax Parameters
- `data_agenzia_entrate`: Future tax agency date (to be added to future calculations)
- `importo_tasse_agenzia`: Hypothetical tax amount (to be determined)

### Benefits & Allowances  
- `aiuto_luxottica_e_bonus_nido`: Monthly welfare reimbursement
  - Deduct from payslip under "assegno unico" category

### Leave Parameters
- `mesi_maternita_facoltativa_Anna`: Maternity leave duration


### Correzioni spese
- `correzioni_spese`: 2023-03-01 è perché abbiamo dato 2000 euro a Carlo, li monitoriamo ma non li contiamo per le previsioni.

## Next Steps, Open Points and Ideas

- Gestione della vendita della shares Luxottica:
  -   Eliminare transazione vecchia delle azioni acquistate
  -   Creare transazione con PROFITTO o PERDITA con solo il profitto netto fatto su tutte le azioni vendute 
  -   (ad ora speso 4 azioni a 181.90 euro acquistate il 6 Novembre 2023)
  -   (aggiunta 4 azioni a 282.05 euro acquistate il 5 Maggio 2025)
- Gestione della vendita di ETF:
  -   Eliminare parte della transazione vecchia degli ETF acquistati
  -   Creare transazione con PROFITTO o PERDITA con solo il profitto netto fatto su tutti gli ETF Venduti
  -   Aggiornare il file con gli etf e le transazioni di Fineco
  -   Aggiornare il config file
- Gestione dell'ETF Monetario / Conto Deposito:
  -   Aggiungere le info di ciò che è stato acquistato di etf in un nuovo file excel e poi mergiarlo
  -   Aggiungere transazione come Conto Deposito
  -   Monitorare l'andamento di quanto c'è dentro e dell'andamento dei Tassi

- Aggiungere uno specchietto con percenutali del portafoglio investito (no conto deposito, no liquidità)
- Capire se 10% oro (magari in futuro)

Ricreare app o modellino goal based per definire l'allocazione ottimale
Scaricare da Fineco, Ordini Contabili, Movimenti Dossier tutti i movimenti passati (MIO e di ANNA)
Aggiungere su MoneyCoach la parte di XEON
Aggiungere su MoneyCoach il resto del portafoglio

Modificare il codice

spaccare portafoglio totale, composizione parte investita e spaccare per region se non c'è