/// Wrapper around Hugging Face Tokenizer.
pub struct Tokenizer {
    tokenizer: tokenizers::Tokenizer
}

impl Tokenizer {
    /// Returns a new Tokenizer instance for a pre-trained model.
    pub async fn new(name: &str) -> Result<Tokenizer, anyhow::Error> {
        match tokenizers::Tokenizer::from_pretrained(name, None) {
            Ok(tokenizer) => {
                Ok(Tokenizer{tokenizer})
            },
            Err(e) => {
                Err(anyhow::anyhow!(e))
            }
        }
    }
    
    /// Returns a list of tokens for the given text. 
    pub async fn encode(&self, text: &str) -> Result<Vec<String>, anyhow::Error> {
        match self.tokenizer.encode(text, false) {
            Ok(encoding) => {
                Ok(encoding.get_tokens().to_vec())
            },
            Err(e) => {
                Err(anyhow::anyhow!(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tokenizer() {
        let tokenizer = Tokenizer::new("bert-base-uncased").await.unwrap();
        let tokens = tokenizer.encode("Hello, world!").await.unwrap();
        assert_eq!(tokens, vec!["hello", ",", "world", "!"]);
    }
}