export type PaginationInput = {
  page?: number;
  pageSize?: number;
};

export type NormalizedPagination = {
  page: number;
  pageSize: number;
  skip: number;
  limit: number;
};

export type PaginationMeta = {
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
  hasPreviousPage: boolean;
  hasNextPage: boolean;
};

export type PaginatedResult<TItem> = {
  items: TItem[];
  pagination: PaginationMeta;
};

export const DEFAULT_PAGE = 1;
export const DEFAULT_PAGE_SIZE = 20;
export const MAX_PAGE_SIZE = 100;

function toSafeInteger(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isInteger(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number.parseInt(value, 10);
    return Number.isInteger(parsed) ? parsed : undefined;
  }

  return undefined;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

export function normalizePagination(
  input: PaginationInput,
): NormalizedPagination {
  const rawPage = toSafeInteger(input.page);
  const rawPageSize = toSafeInteger(input.pageSize);

  const page = rawPage !== undefined && rawPage > 0 ? rawPage : DEFAULT_PAGE;
  const pageSize = clamp(
    rawPageSize !== undefined && rawPageSize > 0
      ? rawPageSize
      : DEFAULT_PAGE_SIZE,
    1,
    MAX_PAGE_SIZE,
  );

  const skip = (page - 1) * pageSize;
  const limit = pageSize;

  return {
    page,
    pageSize,
    skip,
    limit,
  };
}

export function buildPaginationMeta(params: {
  page: number;
  pageSize: number;
  totalItems: number;
}): PaginationMeta {
  const totalItems = Math.max(0, params.totalItems);
  const totalPages =
    totalItems === 0 ? 0 : Math.ceil(totalItems / params.pageSize);

  return {
    page: params.page,
    pageSize: params.pageSize,
    totalItems,
    totalPages,
    hasPreviousPage: params.page > 1,
    hasNextPage: totalPages > 0 && params.page < totalPages,
  };
}

export function buildPaginatedResult<TItem>(params: {
  items: TItem[];
  page: number;
  pageSize: number;
  totalItems: number;
}): PaginatedResult<TItem> {
  return {
    items: params.items,
    pagination: buildPaginationMeta({
      page: params.page,
      pageSize: params.pageSize,
      totalItems: params.totalItems,
    }),
  };
}
