-- Synthetic stand-in for Navi's Postgres, shaped like the Django tables in
-- navi_backend (development branch). Only the columns crumb cares about plus the
-- PII/secret columns it must NOT land (emails, names, phones, tokens, IPs).
drop table if exists
    awards_userawardlevel, awards_useraward, awards_userloyalty, awards_awardlevel,
    awards_award, devices_deviceevent, orders_machineerrorlog, orders_ordercustomization,
    orders_orderitem, payments_invoice, orders_order, payments_payment, devices_naviport,
    devices_espressomachine, devices_machinetype, devices_raspberrypi, menu_customization,
    menu_menuitem, menu_category, users_user
cascade;

create table users_user (
    id                 bigserial primary key,
    password           varchar(128) not null,
    last_login         timestamptz,
    is_superuser       boolean not null default false,
    first_name         varchar(150) not null default '',
    last_name          varchar(150) not null default '',
    is_staff           boolean not null default false,
    is_active          boolean not null default true,
    date_joined        timestamptz not null,
    name               varchar(255) not null default '',
    email              varchar(254) not null unique,
    phone              varchar(32) unique,
    stripe_customer_id varchar(255) unique,
    email_confirmed    boolean not null default false,
    is_guest           boolean not null default false
);

-- AuditModel columns, repeated per table like Django does.
create table menu_category (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table menu_menuitem (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    price numeric(8, 2) not null, description varchar(100) not null default '', body text not null default '',
    view_count integer not null default 0, selected_count integer not null default 0,
    is_featured boolean not null default false, category_id uuid references menu_category(id),
    version integer not null default 1,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table menu_customization (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    group_id uuid, description varchar(255) not null default '', display_order integer not null default 0,
    price numeric(8, 2) not null,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table devices_raspberrypi (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    mac_address varchar(100) not null unique, ip_address inet, location varchar(255) not null default '',
    is_connected boolean not null default false, firmware_version varchar(50) not null default '',
    last_seen timestamptz, token_hash varchar(64) unique, is_simulated boolean not null default false,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table devices_machinetype (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    model_number varchar(100) not null, maintenance_frequency integer not null,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table devices_espressomachine (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    serial_number varchar(100) not null unique, machine_type_id uuid references devices_machinetype(id),
    ip_address inet, port integer, is_online boolean not null default false, last_maintenance_at timestamptz,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table devices_naviport (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    address_line_1 varchar(255) not null default '', address_line_2 varchar(255) not null default '',
    address_line_3 varchar(255) not null default '', city varchar(255) not null default '',
    state_or_region varchar(255) not null default '', postal_code varchar(255) not null default '',
    country varchar(2) not null default 'US',
    espresso_machine_id uuid references devices_espressomachine(id),
    raspberry_pi_id uuid references devices_raspberrypi(id),
    latitude numeric(9, 6), longitude numeric(9, 6),
    accepting_orders boolean not null default true, status_message varchar(255) not null default '',
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table devices_deviceevent (
    id uuid primary key,
    raspberry_pi_id uuid not null references devices_raspberrypi(id),
    type varchar(32) not null, message varchar(255) not null default '',
    payload jsonb not null default '{}'::jsonb,
    actor_id bigint references users_user(id),
    created_at timestamptz not null
);
create index on devices_deviceevent (created_at);

create table payments_payment (
    id uuid primary key,
    stripe_setup_intent_id varchar(255) not null default '',
    stripe_payment_method_id varchar(255) not null default '',
    stripe_payment_intent_id varchar(255) unique,
    amount_received numeric(10, 2) not null default 0, subtotal numeric(10, 2) not null default 0,
    tax_amount numeric(10, 2) not null default 0, total_amount numeric(10, 2) not null default 0,
    currency varchar(10) not null default 'usd', status varchar(20) not null,
    stripe_tax_calculation_id varchar(255) not null default '', stripe_tax_transaction_id varchar(255) not null default '',
    stripe_refund_id varchar(255) not null default '', amount_refunded numeric(10, 2) not null default 0,
    is_deleted boolean not null default false, deleted_at timestamptz,
    last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table orders_order (
    id uuid primary key, slug varchar(200) not null unique,
    user_id bigint references users_user(id), navi_port_id uuid references devices_naviport(id),
    payment_id uuid unique references payments_payment(id),
    cart_token varchar(255) not null, order_status varchar(1) not null default 'O',
    claimed_by_id uuid references devices_raspberrypi(id), claimed_at timestamptz,
    is_test boolean not null default false, qr_nonce varchar(32) not null, cancel_reason text not null default '',
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);
create index on orders_order (updated_at);

create table orders_orderitem (
    id uuid primary key, slug varchar(200) not null unique,
    menu_item_id uuid references menu_menuitem(id), order_id uuid references orders_order(id),
    quantity integer not null default 1, unit_price numeric(8, 2) not null,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table orders_ordercustomization (
    id uuid primary key, slug varchar(200) not null unique,
    order_item_id uuid references orders_orderitem(id), customization_id uuid references menu_customization(id),
    quantity integer not null default 1, unit_price numeric(8, 2) not null,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table orders_machineerrorlog (
    id uuid primary key, order_id uuid not null references orders_order(id),
    raspberry_pi_id uuid references devices_raspberrypi(id),
    error_message text not null, is_recoverable boolean,
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint references users_user(id), updated_by_id bigint references users_user(id)
);

create table payments_invoice (
    id uuid primary key, order_id uuid not null unique references orders_order(id),
    reference_number integer not null unique, pdf varchar(100),
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table awards_award (
    id uuid primary key, name varchar(200) not null unique, slug varchar(200) not null unique,
    description text not null default '', icon varchar(255) not null default '',
    rule_type varchar(32) not null, threshold integer, points_reward integer not null default 0,
    category varchar(32) not null default '',
    status varchar(1) not null default 'A', is_deleted boolean not null default false,
    deleted_at timestamptz, last_modified_ip inet, last_modified_user_agent varchar(200) not null default '',
    created_at timestamptz not null, updated_at timestamptz not null,
    created_by_id bigint not null references users_user(id), updated_by_id bigint not null references users_user(id)
);

create table awards_awardlevel (
    id uuid primary key, award_id uuid not null references awards_award(id),
    rank integer not null, name varchar(100) not null, threshold integer not null,
    points_reward integer not null default 0, icon varchar(255) not null default '',
    unique (award_id, rank)
);

create table awards_userloyalty (
    id uuid primary key, user_id bigint not null unique references users_user(id),
    lifetime_points integer not null default 0, balance_points integer not null default 0,
    orders_completed integer not null default 0, total_spent numeric(12, 2) not null default 0,
    current_tier_id uuid, notifications_enabled boolean not null default true,
    created_at timestamptz not null, updated_at timestamptz not null
);

create table awards_useraward (
    id uuid primary key, user_id bigint not null references users_user(id),
    award_id uuid not null references awards_award(id), level_id uuid references awards_awardlevel(id),
    earned_at timestamptz not null, unique (user_id, award_id)
);

create table awards_userawardlevel (
    id uuid primary key, user_award_id uuid not null references awards_useraward(id),
    level_id uuid not null references awards_awardlevel(id), earned_at timestamptz not null,
    unique (user_award_id, level_id)
);
