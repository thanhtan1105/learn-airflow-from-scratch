CREATE TABLE IF NOT EXISTS public.Customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName VARCHAR(40),
    LastName VARCHAR(40),
    Email VARCHAR(60)
);

CREATE TABLE IF NOT EXISTS public.Accounts (
    AccountID SERIAL PRIMARY KEY,
    CustomerID INT,
    Balance DECIMAL(10, 2),
    AccountType VARCHAR(20),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE IF NOT EXISTS public.Transactions (
    TransactionID SERIAL PRIMARY KEY,
    AccountID INT,
    Amount DECIMAL(10, 2),
    TransactionType VARCHAR(20),
    TransactionDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (AccountID) REFERENCES Accounts(AccountID)
);