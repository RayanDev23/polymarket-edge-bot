# Polymarket Edge Bot — V1

Prototype de recherche quantitative et de paper trading pour les marchés
crypto BTC 5 minutes de Polymarket. V1 est strictement `MODE=PAPER` : elle
ne contient ni clé privée, ni signature, ni endpoint d’envoi d’ordre.

## Installation Windows

Dans PowerShell :

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Les variables de `.env` sont des paramètres publics et de recherche. Ne
mettez jamais de secret, wallet credential ou clé privée dans ce projet.

## Lancement

Découvrir le marché BTC 5m actif et charger ses carnets une fois :

```powershell
python main.py --once
```

Lancer la boucle realtime en paper mode :

```powershell
python main.py
```

Pour un essai borné :

```powershell
python main.py --duration 60
```

Le terminal affiche toujours `MODE=PAPER`. Binance est consommé via le flux
public `bookTicker`. Gamma découvre les marchés dynamiquement et les token IDs
sont lus dans le payload courant; ils ne sont pas hardcodés. Les carnets
Polymarket sont bootstrapés par le CLOB public `/book`, puis suivis via le
market WebSocket public. Si un flux tombe, il se reconnecte et les données
deviennent stale; le risk engine interdit alors toute nouvelle décision.

## Diagnostic réseau Polymarket

Depuis PowerShell Windows :

```powershell
python scripts/polymarket_smoke_test.py
```

Cet outil est strictement en lecture seule. Il vérifie indépendamment DNS,
TLS, HTTP, Content-Type, parsing JSON, découverte de marchés, carnet CLOB et
WebSocket Polymarket. Il récupère le token de test dynamiquement depuis Gamma.
Il ne place aucun ordre, ne nécessite aucune clé privée et ne nécessite aucun
wallet. Un résultat `BLOCKED` identifie la couche réseau qui empêche le test.

## Tests et replay

```powershell
python -m pytest -q
python main.py --replay --replay-limit 1000
```

Le replay lit uniquement les observations déjà enregistrées dans SQLite.
Une décision à l’instant T ne lit pas les observations futures; les données
futures ne servent, au besoin, qu’à une analyse postérieure.

## Architecture

- `data.py` : Binance `bookTicker`, horodatages locaux/exchange, âge, latence,
  reconnexion et historique borné.
- `market.py` : découverte Gamma BTC 5m, parsing des outcomes/token IDs,
  CLOB read-only, order books et estimation par profondeur, WebSocket CLOB.
- `strategy.py` : structural arbitrage UP+DOWN, modèle late-market basé sur une
  probabilité lognormale testable, et features momentum auxiliaires uniquement.
- `risk.py` : capital, exposition, edge/liquidité minimums, stale data,
  latence, pertes et circuit breaker.
- `execution.py` : fills paper agressifs à partir du carnet courant, partial
  fills, frais, slippage, latence et settlement simulé.
- `database.py` : observations, snapshots, opportunités (y compris rejetées),
  trades, positions, statistiques journalières et replay.
- `analytics.py` : PnL brut/net, drawdown, profit factor, Sharpe approximatif,
  découpages et analyse des opportunités non prises.
- `main.py` : orchestration paper-only et CLI.

Les frais de taker sont explicitement modélisés avec la courbe documentée
`quantity × rate × (price × (1 - price))^exponent`. Le taux et l’exposant
fournis par le marché sont préférés; sinon les fallbacks visibles
`POLYMARKET_TAKER_FEE_RATE` et `POLYMARKET_FEE_EXPONENT` sont utilisés. Le
coût d’exécution est calculé niveau par niveau, jamais au midpoint.

## Limitations connues

- La V1 utilise Binance spot BTC/USDT comme référence de recherche, pas comme
  résultat de résolution garanti par Polymarket.
- La disponibilité de `priceToBeat` dépend du payload public courant. Quand ce
  champ n’est pas présent, la stratégie late-market n’est pas activée.
- Le replay repose sur les observations capturées par ce bot; il n’invente pas
  de chandeliers ni de carnets historiques.
- Le modèle probabiliste est un modèle de recherche simple et explicite, pas
  une calibration exécutable ni une garantie de résolution.
- La probabilité de non-remplissage est approchée par la profondeur disponible,
  la latence et les partial fills observés. Elle devra être calibrée avec des
  données de fills réels avant tout usage expérimental plus avancé.
- Le settlement paper live n’est appliqué qu’après réception de l’événement
  officiel `market_resolved`; le replay ne fabrique pas de PnL à partir d’un
  prix Binance postérieur.
- Aucun mécanisme de trading live ne doit être ajouté sans une nouvelle revue
  de sécurité et un environnement séparé.

## Ce bot n'est pas garanti rentable

Un win rate, un midpoint favorable ou un backtest positif ne prouvent pas un
edge. Examinez toujours le PnL net après frais, spread, profondeur,
slippage, partial fills, latence, capital immobilisé et opportunités rejetées.
