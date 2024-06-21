# Apartment Monitor
If you've ever searched for an apartment, you've probably encountered a horribly designed apartment website. Instead of having to manually check many websites, this project enables you to automatically check apartment websites, store apartment data, and provide alerts for pricing and unit inventory changes. Essentially, this is a price/inventory tracker for apartment listings.

## Setup Instructions


Run using cron?

Installing the latest chromedriver version using the webdriver_manager package.

## Customizing
Each website will be different. If you're able to get to a page that has a table, you can use the parse_table_from_html() function to get a Pandas DataFrame with all of the information in the table. Other websites may require more interactivity (such as a load more button). I've used Selenium to handle this.

