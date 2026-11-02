/**
 * মাল্টি-ইউজার অথেনটিকেশন কনফিগারেশন (Multi-User Authentication Configuration)
 * অনুমোদিত ইমেইল বাধ্যবাধকতা তুলে দেওয়া হয়েছে: যে কেউ Google দিয়ে লগইন করতে পারবে।
 */

export const ALLOWED_EMAILS: string[] = [];

/**
 * যেকোনো বৈধ ইমেইলকে প্রবেশের অনুমতি দেয়
 */
export function isEmailAllowed(email?: string | null): boolean {
  return Boolean(email && email.trim().length > 0);
}

/**
 * প্রাথমিক সুপার অ্যাডমিন ইমেইল
 */
export const PRIMARY_ADMIN_EMAIL = 'sobujbd679@gmail.com';

