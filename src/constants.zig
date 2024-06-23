pub const TAB_PAGE_SIZE = 4096;
pub const TAB_MAX_PAGES = 1000;
pub const TAB_ROWS_PER_PAGE = TAB_PAGE_SIZE / ROW_SIZE;
pub const TAB_MAX_ROWS = TAB_ROWS_PER_PAGE * TAB_MAX_PAGES;
pub const ID_SIZE = @sizeOf(u32);
pub const USERNAME_SIZE = 32;
pub const USERNAME_LEN_SIZE = 1;
pub const EMAIL_SIZE = 255;
pub const EMAIL_LEN_SIZE = 1;
pub const ROW_SIZE = ID_SIZE + USERNAME_LEN_SIZE + USERNAME_SIZE + EMAIL_LEN_SIZE + EMAIL_SIZE;
pub const ID_OFF = 0;
pub const USERNAME_LEN_OFF = ID_OFF + ID_SIZE;
pub const USERNAME_OFF = USERNAME_LEN_OFF + USERNAME_LEN_SIZE;
pub const EMAIL_LEN_OFF = USERNAME_OFF + USERNAME_SIZE;
pub const EMAIL_OFF = EMAIL_LEN_OFF + EMAIL_LEN_SIZE;
