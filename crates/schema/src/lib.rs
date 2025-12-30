pub mod alt_resolver;
pub mod dex_swap;
pub mod swap;
pub mod tx_facts;

// Legacy swap event (deprecated, use DexSwapV1)
pub use swap::SwapEvent;

// ALT resolution utilities
pub use alt_resolver::{
    extract_program_ids_from_transaction, pick_main_program, resolve_full_account_keys,
};

// Gold swap contract (v2)
pub use dex_swap::{
    ConfidenceReasons, DexSwapV1, DexSwapV1Builder, RAYDIUM_AMM_V4_PROGRAM_ID, TOKEN_PROGRAM_ID,
};

// TxFacts layer
pub use tx_facts::{ParsedInstruction, TokenBalance, TokenBalanceDelta, TxFacts};
