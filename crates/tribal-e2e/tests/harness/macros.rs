/// Wraps an async seed closure, hiding the HRTB boxing ceremony.
///
/// The body can `.await` repository calls and use `seed.conn()`,
/// `seed.label()`, etc.
///
/// ```ignore
/// seed!(setup, |seed| {
///     let item = PgKnowledgeItemRepository
///         .insert(seed.conn(), &a_new_knowledge_item()...build())
///         .await
///         .expect("insert");
///     seed.label("item", item.id());
/// });
/// ```
///
/// Expands to `setup.seed(Box::new(|seed| Box::pin(async move { ... })))`.
macro_rules! seed {
    ($setup:expr, |$seed:ident| $body:block) => {
        $setup.seed(::std::boxed::Box::new(
            |$seed| ::std::boxed::Box::pin(async move $body),
        ))
    };
}

pub(crate) use seed;
