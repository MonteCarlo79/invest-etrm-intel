# dipeng/DE hourly profile trade

- id: `dipeng_de_hourly_profile_trade` · class: practice · type: folder
- topic: Hourly power price profile trading and PCA-based profile pricing in the German market · level: advanced · market: DE · year: 2016
- worked examples: True · code: True

## Concepts
- **Hourly price profile** — The vector of within-day electricity prices across all hours, treated as a tradeable shape distinct from the flat baseload price.
- **Profile trade** — A bilateral OTC transaction whose value depends on the hourly shape of power prices rather than a flat period price.
- **Principal Component Analysis (PCA)** — Dimensionality-reduction technique applied to historical hourly price or load matrices to extract orthogonal factors explaining cross-hour price co-movement.
- **PCA loadings** — Eigenvectors of the covariance matrix that define how each hour contributes to each principal component and serve as reusable shape factors.
- **PCA scores** — Projected coordinates of each observed daily price profile onto the principal component axes, summarising daily shape realisations.
- **Covariance matrix of hourly prices** — Square matrix capturing pairwise co-movement between individual settlement hours, used as input to eigendecomposition in PCA.
- **Eigenvalue / variance explained** — Magnitude of each principal component's eigenvalue expressed as a percentage of total variance, used to select the number of retained components.
- **Auto-scaling (standardisation)** — Pre-processing step that mean-centres and unit-variance-scales each hourly variable before PCA to remove differences in price level across hours.
- **Profile pricing tool** — Spreadsheet or model that converts a set of PCA loadings and market inputs into a fixed-price matrix for an hourly-shaped power contract.
- **Price forward curve (PFC)** — Hourly or granular forward price curve used as the mark-to-market benchmark against which profile trade day-one P&L is assessed.
- **Day-one P&L / mark-to-market** — Immediate profit or loss on a profile trade computed by revaluing the fixed-price hourly matrix against the prevailing PFC at trade date.
- **Backcast / backtesting of loadings** — Out-of-sample historical simulation applying PCA loadings derived from one period to price profiles in earlier periods to assess model stability.
- **Hourly profile aggregation** — Process of combining individual customer or contract hourly load shapes into a net portfolio profile for pricing or hedging purposes.
- **Score plot** — 2-D scatter of observation scores on selected principal components used to identify clustering or outliers among daily price profiles.
- **Loading plot (biplot component)** — 2-D scatter of variable loadings on selected principal components used to interpret which hours drive each shape factor.

## Methods
- Principal Component Analysis (eigendecomposition of covariance matrix)
- Auto-scaling / standardisation of price matrices
- Eigenvalue decomposition via eig()
- Score matrix computation T = X·P
- Variance-explained ranking of principal components
- Hourly price matrix construction for OTC contract pricing
- Mark-to-market against price forward curve
- Historical backtesting of PCA loadings
- Python scripting (numpy, scipy, matplotlib) for profile analytics
- Excel-based profile pricing and aggregation models

## Implied prerequisites
- Linear algebra (matrix multiplication, eigendecomposition)
- Multivariate statistics (covariance, variance)
- Power market structure and settlement conventions
- Forward curve construction
- OTC energy contract mechanics
- Time-series data handling
- Spreadsheet modelling (Excel/VBA)
- Basic Python / numpy
