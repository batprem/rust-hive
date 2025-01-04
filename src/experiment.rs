use std::thread;
use std::time::Duration;

fn main() {
    // Create the main thread that spawns multiple worker threads
    let main_handle = thread::spawn(|| {
        println!("Main thread started.");

        let mut outer_handles = vec![];

        for i in 0..3 {
            let handle = thread::spawn(move || {
                println!("Outer thread {} started.", i);

                let mut inner_handles = vec![];

                for j in 0..2 {
                    let inner_handle = thread::spawn(move || {
                        println!("Inner thread {}-{} started.", i, j);
                        thread::sleep(Duration::from_millis(500)); // Simulate work
                        println!("Inner thread {}-{} finished.", i, j);
                    });

                    inner_handles.push(inner_handle);
                }

                // Wait for all inner threads to complete
                for inner_handle in inner_handles {
                    inner_handle.join().expect("Inner thread failed to join");
                }

                println!("Outer thread {} finished.", i);
            });

            outer_handles.push(handle);
        }

        // Wait for all outer threads to complete
        for handle in outer_handles {
            handle.join().expect("Outer thread failed to join");
        }

        println!("Main thread finished.");
    });

    // Wait for the main thread to complete
    main_handle.join().expect("Main thread failed to join");
}
