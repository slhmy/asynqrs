use super::*;

#[test]
fn pagination_allows_zero_page_size_like_asynq() {
    let pagination = Pagination::new(3, 0).unwrap();

    assert_eq!(pagination.page(), 3);
    assert_eq!(pagination.page_size(), 0);
    assert_eq!(pagination.start(), 0);
    assert_eq!(pagination.stop(), -1);
}

#[test]
fn pagination_default_matches_asynq_list_defaults() {
    let pagination = Pagination::default();

    assert_eq!(Pagination::DEFAULT_PAGE, 0);
    assert_eq!(Pagination::DEFAULT_PAGE_SIZE, 30);
    assert_eq!(DEFAULT_LIST_PAGE_SIZE, Pagination::DEFAULT_PAGE_SIZE);
    assert_eq!(pagination.page(), 0);
    assert_eq!(pagination.page_size(), 30);
    assert_eq!(pagination.start(), 0);
    assert_eq!(pagination.stop(), 29);
}

#[test]
fn pagination_from_asynq_options_normalizes_like_upstream() {
    let pagination = Pagination::from_asynq_options(3, 10).unwrap();

    assert_eq!(pagination.page(), 2);
    assert_eq!(pagination.page_size(), 10);
    assert_eq!(pagination.start(), 20);
    assert_eq!(pagination.stop(), 29);

    let normalized = Pagination::from_asynq_options(-5, -10).unwrap();

    assert_eq!(normalized.page(), 0);
    assert_eq!(normalized.page_size(), 0);
    assert_eq!(normalized.start(), 0);
    assert_eq!(normalized.stop(), -1);

    let zero_page = Pagination::from_asynq_options(0, 10).unwrap();

    assert_eq!(zero_page.page(), -1);
    assert_eq!(zero_page.page_size(), 10);
    assert_eq!(zero_page.start(), -10);
    assert_eq!(zero_page.stop(), -1);
}

#[test]
fn public_list_options_compose_like_asynq() {
    assert_eq!(page_size(25), ListOption::PageSize(25));
    assert_eq!(page(2), ListOption::Page(2));
    assert_eq!(page_size(-25), ListOption::PageSize(0));
    assert_eq!(page(-2), ListOption::Page(1));
    assert_eq!(page(0), ListOption::Page(0));
    assert_eq!(DEFAULT_LIST_PAGE_NUMBER, 1);
    assert_eq!(DEFAULT_LIST_PAGE_SIZE, 30);

    let default = Pagination::from_list_options([]).unwrap();
    assert_eq!(default.page(), 0);
    assert_eq!(default.page_size(), 30);

    let pagination = Pagination::from_list_options([page_size(10), page(3)]).unwrap();
    assert_eq!(pagination.page(), 2);
    assert_eq!(pagination.page_size(), 10);
    assert_eq!(pagination.start(), 20);
    assert_eq!(pagination.stop(), 29);

    let overridden = Pagination::from_list_options([page_size(10), page_size(5), page(2)]).unwrap();
    assert_eq!(overridden.page(), 1);
    assert_eq!(overridden.page_size(), 5);

    let normalized = Pagination::from_list_options([page_size(-10), page(-5)]).unwrap();
    assert_eq!(normalized.page(), 0);
    assert_eq!(normalized.page_size(), 0);

    let zero_page = Pagination::from_list_options([page_size(10), page(0)]).unwrap();
    assert_eq!(zero_page.page(), -1);
    assert_eq!(zero_page.start(), -10);
    assert_eq!(zero_page.stop(), -1);
}
