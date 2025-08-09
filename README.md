# Momentum Screener - A Personal Analytics Project

Not investment advice.  


This is a personal data project to keep tabs on the stock market.  It downloads data from the Polygon.io api, and organizes it around the concept of "momentum"--or how the stocks have performed relative to others over the past 12 months.  Data refreshes weekly.  The stocks analyzed are those in the SP500 or SP400 indices--that universe of stocks is refreshed weekly from the SSGA daily holdings websites.

Lessons learned in assembling this.
- Lots of time and errors on the type and format--of dates, and floats.  Several issues going in and out of the database.  Should work to get that right next time.
- Building in print lines for debugging was really helpful
- Establishing the flow of

Possible Next projects:
- Industry-specific momentum report: take all the companies relevant to an industry, separate into subsegments, and use that for perspective on the industry as a whole.
- Expand beyond the SP500 and SP400 to all portfolio or watchlist stocks of interest
- Incorporate a more advanced algorithm to assess momentum, or create a quantitative projection of forward return.
- Build in models that incorporate options data or flows.


