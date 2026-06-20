use super::AdminError;

/// Pagination input for Redis-backed Inspector/Admin task listing.
///
/// Reference: Asynq v0.26.0 inspector list APIs use zero-based page numbers
/// and page size to derive Redis range offsets. A page size of zero is passed
/// through to RDB pagination, whose stop offset becomes -1:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>.
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L600-L615>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pagination {
    page: isize,
    page_size: usize,
}

/// Public list option used by Inspector list operations.
///
/// Reference: Asynq v0.26.0 public `ListOption`, `PageSize`, and `Page`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListOption {
    PageSize(isize),
    Page(isize),
}

/// Default Inspector list page size.
///
/// Reference: Asynq v0.26.0 `DEFAULT_LIST_PAGE_SIZE`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
pub const DEFAULT_LIST_PAGE_SIZE: usize = 30;

/// Default one-based Inspector list page number.
///
/// Reference: Asynq v0.26.0 `DEFAULT_LIST_PAGE_NUMBER`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
pub const DEFAULT_LIST_PAGE_NUMBER: isize = 1;

/// Returns a list option to specify the page size for Inspector list calls.
///
/// Reference: Asynq v0.26.0 public `PageSize` constructor:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
pub fn page_size(n: isize) -> ListOption {
    // Reference: Asynq v0.26.0 `PageSize` treats negative page sizes as zero:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
    ListOption::PageSize(n.max(0))
}

/// Returns a list option to specify the one-based page number for list calls.
///
/// Reference: Asynq v0.26.0 public `Page` constructor:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
pub fn page(n: isize) -> ListOption {
    // Reference: Asynq v0.26.0 `Page` treats negative page numbers as one
    // while preserving page zero:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
    ListOption::Page(if n < 0 { 1 } else { n })
}

impl Pagination {
    /// Default zero-based page used by Rust listing calls.
    ///
    /// Reference: Asynq v0.26.0 inspector list APIs default to page number 1,
    /// then convert it to zero-based Redis pagination with `pageNum - 1`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
    pub const DEFAULT_PAGE: usize = 0;

    /// Default page size used by inspector list operations.
    ///
    /// Reference: Asynq v0.26.0 `DEFAULT_LIST_PAGE_SIZE`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
    pub const DEFAULT_PAGE_SIZE: usize = DEFAULT_LIST_PAGE_SIZE;

    pub fn new(page: usize, page_size: usize) -> Result<Self, AdminError> {
        Ok(Self {
            page: page.try_into().unwrap_or(isize::MAX),
            page_size,
        })
    }

    /// Builds pagination from Asynq's public one-based list option semantics.
    ///
    /// Reference: Asynq v0.26.0 `Page`, `PageSize`, and inspector list APIs:
    /// negative page numbers become page 1, page number 0 is preserved, negative
    /// page sizes become 0, and inspector methods convert the resulting page
    /// number to zero-based Redis pagination with `pageNum - 1`.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L323>.
    pub fn from_asynq_options(page_number: isize, page_size: isize) -> Result<Self, AdminError> {
        let page_number = if page_number < 0 { 1 } else { page_number };
        let page_size = page_size.max(0) as usize;
        Ok(Self {
            page: page_number - 1,
            page_size,
        })
    }

    /// Builds pagination from public Asynq-style list options.
    ///
    /// Reference: Asynq v0.26.0 `composeListOptions` applies options in order
    /// with default page size 30 and default page number 1:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L267-L282>.
    pub fn from_list_options<I>(options: I) -> Result<Self, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let mut page_number = DEFAULT_LIST_PAGE_NUMBER;
        let mut page_size = Self::DEFAULT_PAGE_SIZE as isize;
        for option in options {
            match option {
                ListOption::PageSize(size) => page_size = size,
                ListOption::Page(page) => page_number = page,
            }
        }
        Self::from_asynq_options(page_number, page_size)
    }

    pub fn page(&self) -> isize {
        self.page
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn start(&self) -> isize {
        self.page.saturating_mul(page_size_as_isize(self.page_size))
    }

    pub(crate) fn stop(&self) -> isize {
        self.start()
            .saturating_add(page_size_as_isize(self.page_size))
            .saturating_sub(1)
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: Self::DEFAULT_PAGE as isize,
            page_size: Self::DEFAULT_PAGE_SIZE,
        }
    }
}

fn page_size_as_isize(page_size: usize) -> isize {
    page_size.try_into().unwrap_or(isize::MAX)
}
