create table if not exists public.customers
(
    customerid serial
        primary key,
    firstname  varchar(40),
    lastname   varchar(40),
    email      varchar(60)
        constraint customers_pk
            unique
);

alter table public.customers
    owner to airflow;

create table if not exists public.accounts
(
    account_id   integer default nextval('accounts_accountid_seq'::regclass) not null
        primary key,
    customerid   integer
        constraint accounts_unique_customerid
            unique
        references public.customers,
    balance      numeric(10, 2),
    account_type varchar(20)
);

alter table public.accounts
    owner to airflow;

create table if not exists public.transactions
(
    transaction_id   integer   default nextval('transactions_transactionid_seq'::regclass) not null
        primary key,
    account_id       integer
        constraint transactions_accountid_fkey
            references public.accounts,
    amount           numeric(10, 2),
    transaction_type varchar(20),
    transaction_date timestamp default CURRENT_TIMESTAMP
);

alter table public.transactions
    owner to airflow;

