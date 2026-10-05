-- Runs once, the first time the postgres volume is initialised.
--
-- Two databases rather than two schemas in one. A shared schema is the easiest way for an
-- event-driven system to quietly become a distributed monolith: the moment one service can join
-- against another's tables, the event contract stops being the contract.
create database notifications owner wallet;
