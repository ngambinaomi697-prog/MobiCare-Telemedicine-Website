begin;

alter table public.profiles enable row level security;

revoke all privileges on table public.profiles from anon;
revoke all privileges on table public.profiles from authenticated;

grant select, update on table public.profiles to authenticated;

create or replace function public.handle_mobicare_new_user()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into public.profiles (id, full_name, phone, role)
  values (
    new.id,
    nullif(new.raw_user_meta_data ->> 'full_name', ''),
    nullif(new.raw_user_meta_data ->> 'phone', ''),
    'patient'
  )
  on conflict (id) do nothing;
  return new;
end;
$$;

revoke all on function public.handle_mobicare_new_user() from public, anon, authenticated;

drop trigger if exists on_auth_user_created_mobicare on auth.users;
create trigger on_auth_user_created_mobicare
after insert on auth.users
for each row execute function public.handle_mobicare_new_user();

create or replace function public.prevent_mobicare_self_role_change()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if new.role is distinct from old.role
     and coalesce(current_setting('request.jwt.claim.role', true), '') <> 'service_role' then
    raise exception 'Profile role changes require administrator authorization'
      using errcode = '42501';
  end if;
  return new;
end;
$$;

revoke all on function public.prevent_mobicare_self_role_change() from public, anon, authenticated;

drop trigger if exists prevent_mobicare_self_role_change on public.profiles;
create trigger prevent_mobicare_self_role_change
before update of role on public.profiles
for each row execute function public.prevent_mobicare_self_role_change();

do $$
declare policy_row record;
begin
  for policy_row in
    select policyname from pg_policies
    where schemaname = 'public' and tablename = 'profiles'
  loop
    execute format('drop policy %I on public.profiles', policy_row.policyname);
  end loop;
end;
$$;

create policy profiles_select_own
on public.profiles for select to authenticated
using ((select auth.uid()) = id);

create policy profiles_update_own
on public.profiles for update to authenticated
using ((select auth.uid()) = id)
with check ((select auth.uid()) = id);

commit;
