
bitflags! {
    pub struct Flags: u32 {
        const HAS_TIMESTAMP  = 0b00000001;
        const HAS_PROPERTIES = 0b00000010;
        const HAS_BODY       = 0b00000100;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    
    #[test]
    fn flags_empty() {
        let empty = Flags::empty();

        assert!(empty.is_empty());
    }

    #[test]
    fn add_flags_to_empty() {
        let mut flags = Flags::empty();
        flags.insert(Flags::HAS_TIMESTAMP);
        assert_eq!(flags, Flags::HAS_TIMESTAMP);
        assert!(flags.contains(Flags::HAS_TIMESTAMP));
        assert!(!flags.contains(Flags::HAS_BODY));
        flags.insert(Flags::HAS_BODY);
        assert!(flags.contains(Flags::HAS_TIMESTAMP | Flags::HAS_BODY));
    }

    #[test]
    fn flags_to_int() {
        let mut flags = Flags::empty();
        assert_eq!(0, flags.bits());
        flags.insert(Flags::HAS_TIMESTAMP);
        assert_eq!(1, flags.bits());
        flags.insert(Flags::HAS_PROPERTIES);
        assert_eq!(3, flags.bits());
        flags.insert(Flags::HAS_BODY);
        assert_eq!(7, flags.bits());
        assert_eq!(2, Flags::HAS_PROPERTIES.bits());
        assert_eq!(4, Flags::HAS_BODY.bits());
    }

    #[test]
    fn int_to_flags() {
        assert_eq!(Flags::from_bits(4).unwrap(), Flags::HAS_BODY);
        assert_eq!(Flags::from_bits(6).unwrap(), Flags::HAS_BODY | Flags::HAS_PROPERTIES);
        assert_eq!(Flags::from_bits(7).unwrap(), Flags::all());
    }
}