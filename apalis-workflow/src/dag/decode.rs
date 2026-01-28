use apalis_core::backend::{BackendExt, codec::Codec};

/// An entry trait that handles the values of a fan-in/fanout entry
pub trait DagCodec<B: BackendExt>: Sized {
    /// The codec error
    type Error;

    /// Encode the input into a Vec<Compact>
    fn encode(self) -> Result<B::Compact, Self::Error>;

    /// Decode the previous input
    fn decode(response: &B::Compact) -> Result<Self, Self::Error>;
}

impl<B, T, Err> DagCodec<B> for Vec<T>
where
    B: BackendExt,
    B::Codec: Codec<T, Compact = B::Compact, Error = Err>
        + Codec<Vec<B::Compact>, Compact = B::Compact, Error = Err>
        + Codec<Self, Compact = B::Compact, Error = Err>,
{
    type Error = Err;
    fn encode(self) -> Result<B::Compact, <B::Codec as Codec<Self>>::Error> {
        let mut result = Vec::new();
        for input in self {
            result.push(B::Codec::encode(&input)?);
        }
        let compact = B::Codec::encode(&result)?;
        Ok(compact)
    }

    fn decode(response: &B::Compact) -> Result<Self, Err> {
        let decoded = B::Codec::decode(response)?;
        Ok(decoded)
    }
}

macro_rules! impl_entry_for_tuple {
    // First, implement for the first type to extract the error type
    ($T1:ident) => {
        impl<B, Err, $T1> DagCodec<B> for ($T1,)
        where
            B: BackendExt,
            B::Codec: Codec<($T1,), Compact = B::Compact, Error = Err> + Codec<Vec<B::Compact>, Compact = B::Compact, Error = Err>,
        {
            type Error = Err;

            fn encode(self) -> Result<B::Compact, Self::Error> {
                let result = vec![B::Codec::encode(&self)?];
                let compact = B::Codec::encode(&result)?;
                Ok(compact)
            }
            fn decode(response: &B::Compact) -> Result<Self, Err> {
                let decoded = B::Codec::decode(response)?;
                Ok(decoded)
            }
        }
    };

    // For 2+ types, use the first type's error
    ($T1:ident, $($T:ident),+) => {
        impl<B, Err, $T1, $($T),+> DagCodec<B> for ($T1, $($T),+)
        where
            B: BackendExt,
            B::Codec: Codec<$T1, Compact = B::Compact, Error = Err> + Codec<Vec<B::Compact>, Compact = B::Compact, Error = Err>,
            $(B::Codec: Codec<$T, Compact = B::Compact, Error = Err>,)+
            // Ensure all errors are the same type as T1's error
            $(
                <B::Codec as Codec<$T>>::Error: Into<<B::Codec as Codec<$T1>>::Error>,
            )+
        {
            type Error = <B::Codec as Codec<$T1>>::Error;

            fn encode(self) -> Result<B::Compact, Self::Error> {
                #[allow(non_snake_case)]
                let ($T1, $($T),+) = self;

                let mut result = vec![B::Codec::encode(&$T1)?];
                $(
                    result.push(B::Codec::encode(&$T).map_err(Into::into)?);
                )+
                let compact = B::Codec::encode(&result)?;
                Ok(compact)
            }

            fn decode(response: &B::Compact) -> Result<Self, Err> {
                let decoded: Vec<B::Compact> = B::Codec::decode(response)?;
                // Count the number of types in the tuple
                let expected_len = 1 $(+ {let _ = stringify!($T); 1})+;
                if decoded.len() != expected_len {
                    panic!("Expected {} elements, got {}", expected_len, decoded.len());
                }

                let mut iter = decoded.into_iter();

                #[allow(non_snake_case)]
                let $T1 = B::Codec::decode(&iter.next().unwrap())?;
                $(
                    #[allow(non_snake_case)]
                    let $T = B::Codec::decode(&iter.next().unwrap()).map_err(Into::into)?;
                )+

                Ok(($T1, $($T),+))
            }
        }
    };
}

impl_entry_for_tuple!(T1);
impl_entry_for_tuple!(T1, T2);
impl_entry_for_tuple!(T1, T2, T3);
impl_entry_for_tuple!(T1, T2, T3, T4);
impl_entry_for_tuple!(T1, T2, T3, T4, T5);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_entry_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

macro_rules! impl_entry_passthrough {
    ($($T:ty),+) => {
        $(
            impl<B, Err> DagCodec<B> for $T
            where
                B: BackendExt,
                B::Codec: Codec<$T, Compact = B::Compact, Error = Err>,
            {
                type Error = Err;

                fn encode(self) -> Result<B::Compact, Self::Error> {
                    B::Codec::encode(&self)
                }

                fn decode(response: &B::Compact) -> Result<Self, Self::Error> {
                    B::Codec::decode(response)
                }
            }
        )+
    };
}

impl_entry_passthrough!(
    String,
    &'static str,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    bool,
    char,
    ()
);

impl<B, T, Err> DagCodec<B> for Option<T>
where
    B: BackendExt,
    B::Codec: Codec<Self, Compact = B::Compact, Error = Err>,
{
    type Error = Err;

    fn encode(self) -> Result<B::Compact, Self::Error> {
        B::Codec::encode(&self)
    }

    fn decode(response: &B::Compact) -> Result<Self, Self::Error> {
        B::Codec::decode(response)
    }
}

impl<B, T, E, Err> DagCodec<B> for Result<T, E>
where
    B: BackendExt,
    B::Codec: Codec<Self, Compact = B::Compact, Error = Err>,
{
    type Error = Err;

    fn encode(self) -> Result<B::Compact, Self::Error> {
        B::Codec::encode(&self)
    }

    fn decode(response: &B::Compact) -> Result<Self, Self::Error> {
        B::Codec::decode(response)
    }
}
