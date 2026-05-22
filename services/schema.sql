create table customers (
customer_id serial primary key,
first_name varchar(100) not null,
last_name varchar(100) not null,
email varchar(100) not null,
created_at timestamp with time zone default now()
);

create table account (
account_id serial primary key,
customer_id int not null references customers(customer_id) on delete cascade,
account_type varchar(50) not null,
balance numeric(10,2) not null default 0 check (balance >= 0),
currency char(3) not null default 'USD',
created_at TIMESTAMP with TIME zone default now()
);


create table transactions (
    transaction_id bigserial primary key,
    account_id int not null references account(account_id) on delete cascade,
    run_type varchar(50) not null,
    amount numeric(18,2) not null check (amount > 0),
    related_account_id int null,
    status varchar(20) not null default 'COMPLETED',
    created_at timestamp with time zone default now()
);