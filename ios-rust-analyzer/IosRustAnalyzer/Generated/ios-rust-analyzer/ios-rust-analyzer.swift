
public class RustApp: RustAppRefMut {
    var isOwned: Bool = true

    public override init(ptr: UnsafeMutableRawPointer) {
        super.init(ptr: ptr)
    }

    deinit {
        if isOwned {
            __swift_bridge__$RustApp$_free(ptr)
        }
    }
}
extension RustApp {
    public convenience init() {
        self.init(ptr: __swift_bridge__$RustApp$new())
    }
}
public class RustAppRefMut: RustAppRef {
    public override init(ptr: UnsafeMutableRawPointer) {
        super.init(ptr: ptr)
    }
}
public class RustAppRef {
    var ptr: UnsafeMutableRawPointer

    public init(ptr: UnsafeMutableRawPointer) {
        self.ptr = ptr
    }
}
extension RustAppRef {
    public func generate_html<GenericToRustStr: ToRustStr>(_ rust_code: GenericToRustStr) -> RustString {
        return rust_code.toRustStr({ rust_codeAsRustStr in
            RustString(ptr: __swift_bridge__$RustApp$generate_html(ptr, rust_codeAsRustStr))
        })
    }
}
extension RustApp: Vectorizable {
    public static func vecOfSelfNew() -> UnsafeMutableRawPointer {
        __swift_bridge__$Vec_RustApp$new()
    }

    public static func vecOfSelfFree(vecPtr: UnsafeMutableRawPointer) {
        __swift_bridge__$Vec_RustApp$drop(vecPtr)
    }

    public static func vecOfSelfPush(vecPtr: UnsafeMutableRawPointer, value: RustApp) {
        __swift_bridge__$Vec_RustApp$push(vecPtr, {value.isOwned = false; return value.ptr;}())
    }

    public static func vecOfSelfPop(vecPtr: UnsafeMutableRawPointer) -> Optional<Self> {
        let pointer = __swift_bridge__$Vec_RustApp$pop(vecPtr)
        if pointer == nil {
            return nil
        } else {
            return (RustApp(ptr: pointer!) as! Self)
        }
    }

    public static func vecOfSelfGet(vecPtr: UnsafeMutableRawPointer, index: UInt) -> Optional<RustAppRef> {
        let pointer = __swift_bridge__$Vec_RustApp$get(vecPtr, index)
        if pointer == nil {
            return nil
        } else {
            return RustAppRef(ptr: pointer!)
        }
    }

    public static func vecOfSelfGetMut(vecPtr: UnsafeMutableRawPointer, index: UInt) -> Optional<RustAppRefMut> {
        let pointer = __swift_bridge__$Vec_RustApp$get_mut(vecPtr, index)
        if pointer == nil {
            return nil
        } else {
            return RustAppRefMut(ptr: pointer!)
        }
    }

    public static func vecOfSelfLen(vecPtr: UnsafeMutableRawPointer) -> UInt {
        __swift_bridge__$Vec_RustApp$len(vecPtr)
    }
}



